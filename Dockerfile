ARG BASE_IMAGE=python:3.13-slim
FROM ${BASE_IMAGE}

# Set work directory
WORKDIR /app

# Optional custom CA certificate support.
# The cert is supplied via a named build context called "certs" (see
# docker-compose.yml's `additional_contexts`), which can point at any
# directory on the host - independent of this repo's own build context.
# Pass the file's name within that directory as the CERT_FILE build arg,
# e.g. `--build-arg CERT_FILE=my_certificate.crt`. Left unset (default
# points to an empty placeholder) this is a no-op, so setups without a
# custom CA are unaffected.
ARG CERT_FILE=empty.crt
COPY --from=certs ${CERT_FILE} /usr/local/share/ca-certificates/custom-cert.crt

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential git ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# If a non-empty certificate was provided
RUN if [ -s /usr/local/share/ca-certificates/custom-cert.crt ]; then \
        update-ca-certificates && \
        pip config set global.cert /usr/local/share/ca-certificates/custom-cert.crt; \
    fi

# Install uv (fast Python package manager)
RUN pip install --no-cache-dir uv

# uv bundles its own root certificates by default; make it trust the system CA
ENV UV_NATIVE_TLS=1

# Copy project files
COPY pyproject.toml README.md uv.lock ./
COPY src/ ./src/
COPY config/ ./config/

# Install project dependencies
RUN uv sync

# Add the installed packages to PATH
ENV PATH="/app/.venv/bin:$PATH"

# Entry point for the container
ENTRYPOINT ["pos"]

# Default command (can be overridden)
CMD ["--help"]
