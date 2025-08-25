# docker build -t clickhouse/claude .
FROM node:20-bookworm-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    git \
    curl \
    ca-certificates \
    python3 \
    python3-pip && \
    rm -rf /var/lib/apt/lists/*

# Install claude-code
RUN npm install -g @anthropic-ai/claude-code

ARG CACHE_INVALIDATOR=0

WORKDIR /
