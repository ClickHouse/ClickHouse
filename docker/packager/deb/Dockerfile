# docker build -t yandex/clickhouse-deb-builder .
FROM ubuntu:20.04

RUN apt-get --allow-unauthenticated update -y && apt-get install --yes wget gnupg
RUN wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add -
RUN echo "deb [trusted=yes] http://apt.llvm.org/focal/ llvm-toolchain-focal-11 main" >> /etc/apt/sources.list

# initial packages
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

# Special dpkg-deb (https://github.com/ClickHouse-Extras/dpkg) version which is able
# to compress files using pigz (https://zlib.net/pigz/) instead of gzip.
# Significantly increase deb packaging speed and compatible with old systems
RUN curl -O https://clickhouse-builds.s3.yandex.net/utils/1/dpkg-deb
RUN chmod +x dpkg-deb
RUN cp dpkg-deb /usr/bin

RUN apt-get --allow-unauthenticated update -y \
    && env DEBIAN_FRONTEND=noninteractive \
        apt-get --allow-unauthenticated install --yes --no-install-recommends \
            gcc-10 \
            g++-10 \
            gcc-9 \
            g++-9 \
            llvm-11 \
            clang-11 \
            lld-11 \
            clang-tidy-11 \
            llvm-10 \
            clang-10 \
            lld-10 \
            clang-tidy-10 \
            clang-9 \
            lld-9 \
            clang-tidy-9 \
            ninja-build \
            perl \
            pkg-config \
            devscripts \
            debhelper \
            git \
            tzdata \
            gperf \
            alien \
            cmake \
            gdb  \
            moreutils


# This symlink required by gcc to find lld compiler
RUN ln -s /usr/bin/lld-10 /usr/bin/ld.lld

COPY build.sh /

CMD ["/bin/bash", "/build.sh"]
