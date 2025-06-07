# docker build -t clickhouse/performance-comparison .

ARG FROM_TAG=latest
FROM clickhouse/test-base:$FROM_TAG

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install --yes --no-install-recommends \
            bash \
            curl \
            dmidecode \
            g++ \
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
            rsync \
            tree \
            tzdata \
            vim \
            wget \
            rustc \
            cargo \
            ripgrep \
            zstd \
            unzip \
    && apt-get purge --yes python3-dev g++ \
    && apt-get autoremove --yes \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*

COPY requirements.txt /
RUN pip3 --no-cache-dir install -r requirements.txt

RUN mkdir /fg && cd /fg \
    && wget -nv -nd -c "https://raw.githubusercontent.com/brendangregg/FlameGraph/cd9ee4c4449775a2f867acf31c84b7fe4b132ad5/flamegraph.pl" \
    && wget -nv -nd -c "https://raw.githubusercontent.com/brendangregg/FlameGraph/cd9ee4c4449775a2f867acf31c84b7fe4b132ad5/difffolded.pl" \
    && chmod +x /fg/difffolded.pl \
    && chmod +x /fg/flamegraph.pl

COPY run.sh /

COPY --from=clickhouse/cctools:425d6927d1efbd790bdb /opt/gdb /opt/gdb
ENV PATH="/opt/gdb/bin:${PATH}"

# aws cli to acquire secrets and params from ssm
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-$(uname -m).zip" -o "awscliv2.zip" \
    && unzip -q awscliv2.zip && ./aws/install \
    && rm -rf /awscliv2.zip /aws

CMD ["bash", "/run.sh"]

# docker run --network=host --volume <workspace>:/workspace --volume=<output>:/output -e PR_TO_TEST=<> -e SHA_TO_TEST=<> clickhouse/performance-comparison
