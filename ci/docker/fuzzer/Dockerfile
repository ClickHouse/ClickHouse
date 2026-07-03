# rebuild in #33610
# docker build -t clickhouse/fuzzer .
ARG FROM_TAG=latest
FROM clickhouse/test-base:$FROM_TAG

ENV LANG=C.UTF-8

# Install additional packages needed for fuzzer that are not in test-base
RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install --yes --no-install-recommends \
            libc6-dbg \
            ncdu \
            p7zip-full \
            parallel \
            psmisc \
            python3 \
            python3-pip \
            rsync \
            tree \
            vim \
            awscli \
    && apt-get autoremove --yes \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*

COPY requirements.txt /
RUN pip3 install --no-cache-dir -r /requirements.txt

COPY * /

SHELL ["/bin/bash", "-c"]
CMD set -o pipefail \
    && cd /workspace \
    && timeout --verbose --signal 9 1h /run-fuzzer.sh 2>&1 | ts "$(printf '%%Y-%%m-%%d %%H:%%M:%%S\t')" | tee main.log

# docker run --network=host --volume <workspace>:/workspace -e PR_TO_TEST=<> -e SHA_TO_TEST=<> clickhouse/fuzzer
