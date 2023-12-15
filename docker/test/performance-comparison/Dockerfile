# docker build -t clickhouse/performance-comparison .

ARG FROM_TAG=latest
FROM clickhouse/test-base:$FROM_TAG

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install --yes --no-install-recommends \
            bash \
            curl \
            dmidecode \
            g++ \
            gdb \
            git \
            gnuplot \
            imagemagick \
            libc6-dbg \
            moreutils \
            ncdu \
            numactl \
            p7zip-full \
            parallel \
            psmisc \
            python3 \
            python3-dev \
            python3-pip \
            python3-setuptools \
            rsync \
            tree \
            tzdata \
            vim \
            wget \
            rustc \
            cargo \
            ripgrep \
            zstd \
    && pip3 --no-cache-dir install 'clickhouse-driver==0.2.1' scipy \
    && apt-get purge --yes python3-dev g++ \
    && apt-get autoremove --yes \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY * /

# Bind everything to one NUMA node, if there's more than one. Theoretically the
# node #0 should be less stable because of system interruptions. We bind
# randomly to node 1 or 0 to gather some statistics on that. We have to bind
# both servers and the tmpfs on which the database is stored. How to do it
# is unclear, but by default tmpfs uses
# 'process allocation policy', not sure which process but hopefully the one that
# writes to it, so just bind the downloader script as well.
# https://www.kernel.org/doc/Documentation/filesystems/tmpfs.txt
# Double-escaped backslashes are a tribute to the engineering wonder of docker --
# it gives '/bin/sh: 1: [bash,: not found' otherwise.
CMD ["bash", "-c", "node=$((RANDOM % $(numactl --hardware | sed -n 's/^.*available:\\(.*\\)nodes.*$/\\1/p'))); echo Will bind to NUMA node $node; numactl --cpunodebind=$node --membind=$node /entrypoint.sh"]

# docker run --network=host --volume <workspace>:/workspace --volume=<output>:/output -e PR_TO_TEST=<> -e SHA_TO_TEST=<> clickhouse/performance-comparison
