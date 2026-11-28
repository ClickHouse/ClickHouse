# rebuild in #33610
# docker build -t clickhouse/fuzzer .
ARG FROM_TAG=latest
FROM clickhouse/test-base:$FROM_TAG

ENV LANG=C.UTF-8

# Install additional packages needed for fuzzer that are not in test-base
RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install --yes --no-install-recommends \
            lsof \
            ncdu \
            p7zip-full \
            parallel \
            psmisc \
            rsync \
            tree \
            unzip \
            vim \
    && apt-get autoremove --yes \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*

# The awscli apt package is gone in ubuntu:24.04, install a pinned AWS CLI v2
ARG AWSCLI_VERSION=2.36.41
ARG AWSCLI_SHA256_X86_64=659c1fe734a9534736fa452bb1b557dd4cccadea5823f45a74c0f09408a93d22
ARG AWSCLI_SHA256_AARCH64=1612cda15d605985d8cb675ec429c8916e3b696fa52325994955dc3215c590a9
RUN arch="$(uname -m)" \
    && case "$arch" in \
        x86_64) sha256="$AWSCLI_SHA256_X86_64" ;; \
        aarch64) sha256="$AWSCLI_SHA256_AARCH64" ;; \
        *) echo "Unsupported architecture: $arch" >&2; exit 1 ;; \
    esac \
    && curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-${arch}-${AWSCLI_VERSION}.zip" -o awscliv2.zip \
    && echo "$sha256  awscliv2.zip" | sha256sum --check --strict \
    && unzip -q awscliv2.zip \
    && ./aws/install \
    && rm -rf awscliv2.zip aws

# S3 (SeaweedFS) and Azure (azurite-rs) emulators, so fuzzed object storage queries reach the server
# code instead of failing to connect. Keep the versions in sync with `ci/docker/stateless-test`.
ARG SEAWEEDFS_VERSION=4.48
ARG AZURITE_RS_VERSION=v0.1.0-1bc0ba9
ARG TARGETARCH
RUN arch=${TARGETARCH:-amd64} \
    && curl -fL --retry 5 --retry-all-errors --retry-delay 5 --connect-timeout 60 --max-time 600 \
        "https://github.com/seaweedfs/seaweedfs/releases/download/${SEAWEEDFS_VERSION}/linux_${arch}.tar.gz" -o /tmp/seaweedfs.tar.gz \
    && tar -xzf /tmp/seaweedfs.tar.gz -C / weed \
    && rm /tmp/seaweedfs.tar.gz \
    && chmod +x /weed \
    && curl -fsSL "https://github.com/ClickHouse/azurite-rs/releases/download/${AZURITE_RS_VERSION}/azurite-rs-linux-${arch}.tar.gz" | tar -xz -C /usr/local/bin/ \
    && mv /usr/local/bin/azurite-rs-linux-${arch} /usr/local/bin/azurite-rs \
    && chmod +x /usr/local/bin/azurite-rs

COPY requirements.txt /
RUN python3.13 -m pip install --no-cache-dir -r /requirements.txt

COPY * /

SHELL ["/bin/bash", "-c"]
CMD set -o pipefail \
    && cd /workspace \
    && timeout --verbose --signal 9 1h /run-fuzzer.sh 2>&1 | ts "$(printf '%%Y-%%m-%%d %%H:%%M:%%S\t')" | tee main.log

# docker run --network=host --volume <workspace>:/workspace -e PR_TO_TEST=<> -e SHA_TO_TEST=<> clickhouse/fuzzer
