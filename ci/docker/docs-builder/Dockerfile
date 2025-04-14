# docker build -t clickhouse/docs-builder .

# To match the same environment that Vercel is using
FROM amazonlinux:2023

RUN dnf update -y

# Install dependencies using dnf (Amazon Linux 2023 package manager)
RUN dnf install -y --allowerasing \
    git \
    openssh-clients \
    python3 \
    python3-pip \
    rsync \
    curl \
    ca-certificates \
    gcc \
    gcc-c++ \
    make \
    nodejs \
    tar \
    gzip
    
RUN npm install --global yarn && \
    dnf clean all && \
    rm -rf /var/cache/dnf /root/.npm /tmp/*

ARG CACHE_INVALIDATOR=0

# Create a specific user and group
RUN groupadd -r clickhouse && \
    useradd -r -g clickhouse -m -d /home/clickhouse -s /sbin/nologin -c "ClickHouse user" clickhouse

# Set yarn registry (Needs yarn installed first, moved install earlier)
RUN yarn config set registry https://registry.npmjs.org
ENV HOME=/opt/clickhouse-docs

# Create directory with specific ownership
RUN mkdir -p /opt/clickhouse-docs && \
    chown clickhouse:clickhouse /opt/clickhouse-docs && \
    chmod -R a+rw /opt/clickhouse-docs/

# Set working directory
WORKDIR /
