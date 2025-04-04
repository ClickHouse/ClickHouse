# docker build -t clickhouse/docs-builder .
FROM node:20-bookworm-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    git \
    openssh-client \
    python3 \
    ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# So we reset the cache
ARG CACHE_INVALIDATOR=0

RUN git config --global url."https://github.com/".insteadOf git@github.com:

RUN git clone https://github.com/ClickHouse/clickhouse-docs.git \
    --depth=1 --branch=main /opt/clickhouse-docs

RUN yarn config set registry https://registry.npmjs.org \
    && yarn install \
    && yarn cache clean
