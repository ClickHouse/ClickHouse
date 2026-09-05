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

# The awscli apt package is gone in ubuntu:24.04, install AWS CLI v2
# the same way as the fasttest and stateless-test images
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-$(uname -m).zip" -o "awscliv2.zip" \
    && unzip -q awscliv2.zip \
    && ./aws/install \
    && rm -rf awscliv2.zip aws

COPY requirements.txt /
RUN python3.13 -m pip install --ignore-installed --no-cache-dir -r /requirements.txt

COPY * /

SHELL ["/bin/bash", "-c"]
CMD set -o pipefail \
    && cd /workspace \
    && timeout --verbose --signal 9 1h /run-fuzzer.sh 2>&1 | ts "$(printf '%%Y-%%m-%%d %%H:%%M:%%S\t')" | tee main.log

# docker run --network=host --volume <workspace>:/workspace -e PR_TO_TEST=<> -e SHA_TO_TEST=<> clickhouse/fuzzer
