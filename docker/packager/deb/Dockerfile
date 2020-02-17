# docker build -t yandex/clickhouse-deb-builder .
FROM ubuntu:18.04

RUN apt-get --allow-unauthenticated update -y \
    && env DEBIAN_FRONTEND=noninteractive \
        apt-get --allow-unauthenticated install --yes --no-install-recommends \
            bash \
            fakeroot \
            ccache \
            curl \
            software-properties-common \
            gnupg \
            apt-transport-https \
            ca-certificates


RUN echo "deb [trusted=yes] http://apt.llvm.org/bionic/ llvm-toolchain-bionic-8 main" >> /etc/apt/sources.list
RUN add-apt-repository ppa:ubuntu-toolchain-r/test

RUN apt-get --allow-unauthenticated update -y \
    && env DEBIAN_FRONTEND=noninteractive \
        apt-get --allow-unauthenticated install --yes --no-install-recommends \
            gcc-8 \
            g++-8 \
            gcc-9 \
            g++-9 \
            clang-8 \
            lld-8 \
            libclang-8-dev \
            liblld-8-dev \
            libicu-dev \
            libreadline-dev \
            gperf \
            ninja-build \
            perl \
            pkg-config \
            devscripts \
            debhelper \
            git \
            libc++-dev \
            libc++abi-dev \
            libboost-program-options-dev \
            libboost-system-dev \
            libboost-filesystem-dev \
            libboost-thread-dev \
            zlib1g-dev \
            liblz4-dev \
            libdouble-conversion-dev \
            librdkafka-dev \
            libpoconetssl50 \
            libpoco-dev \
            libgoogle-perftools-dev \
            libzstd-dev \
            libltdl-dev \
            libre2-dev \
            libjemalloc-dev \
            unixodbc-dev \
            odbcinst \
            tzdata \
            gperf \
            alien \
            libcapnp-dev \
            cmake \
            gdb  \
            pigz \
            moreutils



# Special dpkg-deb (https://github.com/ClickHouse-Extras/dpkg) version which is able
# to compress files using pigz (https://zlib.net/pigz/) instead of gzip.
# Significantly increase deb packaging speed and compatible with old systems
RUN curl -O https://clickhouse-builds.s3.yandex.net/utils/dpkg-deb
RUN chmod +x dpkg-deb
RUN cp dpkg-deb /usr/bin

RUN ln -s /usr/bin/lld-8 /usr/bin/ld.lld

COPY build.sh /

CMD ["/bin/bash", "/build.sh"]
