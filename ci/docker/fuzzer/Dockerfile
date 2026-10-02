# rebuild in #33610
# docker build -t clickhouse/fuzzer .
ARG FROM_TAG=latest
FROM clickhouse/test-base:$FROM_TAG

ENV LANG=C.UTF-8

# Install additional packages needed for fuzzer that are not in test-base
RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install --yes --no-install-recommends \
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

COPY requirements.txt /
RUN python3.13 -m pip install --no-cache-dir -r /requirements.txt

COPY * /

SHELL ["/bin/bash", "-c"]
CMD set -o pipefail \
    && cd /workspace \
    && timeout --verbose --signal 9 1h /run-fuzzer.sh 2>&1 | ts "$(printf '%%Y-%%m-%%d %%H:%%M:%%S\t')" | tee main.log

# docker run --network=host --volume <workspace>:/workspace -e PR_TO_TEST=<> -e SHA_TO_TEST=<> clickhouse/fuzzer
