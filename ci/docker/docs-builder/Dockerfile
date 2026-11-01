# docker build -t clickhouse/docs-builder .
FROM node:22-bookworm-slim

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y --no-install-recommends \
    git \
    openssh-client \
    python3 \
    python3-venv \
    rsync \
    curl \
    ca-certificates \
    build-essential && \
    rm -rf /var/lib/apt/lists/*

ARG CACHE_INVALIDATOR=0

# Create a specific user and group
RUN groupadd -r clickhouse && \
    useradd -r -g clickhouse -m -d /home/clickhouse -s /sbin/nologin -c "ClickHouse user" clickhouse

RUN npm install -g npm@11 mint@latest

# lychee (link checker) drives the broken-links / anchors / redirects checks.
# Build it from source with cargo rather than downloading a release binary.
RUN curl -sSf 'https://sh.rustup.rs' | sh -s -- -y --profile minimal && \
    apt-get update && \
    apt-get install -y --no-install-recommends pkg-config libssl-dev && \
    . "$HOME/.cargo/env" && \
    cargo install lychee && \
    cp "$HOME/.cargo/bin/lychee" /usr/local/bin/lychee && \
    rustup self uninstall -y && \
    rm -rf /var/lib/apt/lists/*

ENV HOME=/opt/clickhouse-docs

# Create directory with specific ownership
RUN mkdir -p /opt/clickhouse-docs && \
    chown clickhouse:clickhouse /opt/clickhouse-docs && \
    chmod -R a+rw /opt/clickhouse-docs/

# Set working directory
WORKDIR /
