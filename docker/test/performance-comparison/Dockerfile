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
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*

COPY run.sh /

CMD ["bash", "/run.sh"]

# docker run --network=host --volume <workspace>:/workspace --volume=<output>:/output -e PR_TO_TEST=<> -e SHA_TO_TEST=<> clickhouse/performance-comparison
