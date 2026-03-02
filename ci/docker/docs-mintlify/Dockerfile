# docker build -t clickhouse/docs-mintlify .
FROM node:22-bookworm-slim

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    git \
    ca-certificates && \
    rm -rf /var/lib/apt/lists/*

ARG CACHE_INVALIDATOR=0

RUN npm install -g mintlify@latest

WORKDIR /docs
