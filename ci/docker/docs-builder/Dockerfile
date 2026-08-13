# docker build -t clickhouse/docs-builder .
FROM node:20-bookworm-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    git \
    openssh-client \
    python3 \
    python3-venv \
    rsync \
    curl \
    ca-certificates \
    build-essential \
    g++ \
    make && \
    rm -rf /var/lib/apt/lists/*

ARG CACHE_INVALIDATOR=0

# Create a specific user and group
RUN groupadd -r clickhouse && \
    useradd -r -g clickhouse -m -d /home/clickhouse -s /sbin/nologin -c "ClickHouse user" clickhouse

RUN yarn config set registry https://registry.npmjs.org
ENV HOME=/opt/clickhouse-docs

# Create directory with specific ownership
RUN mkdir -p /opt/clickhouse-docs && \
    chown clickhouse:clickhouse /opt/clickhouse-docs && \
    chmod -R a+rw /opt/clickhouse-docs/

# Set working directory
WORKDIR /
