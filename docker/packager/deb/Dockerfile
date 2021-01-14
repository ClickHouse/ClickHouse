# docker build -t yandex/clickhouse-deb-builder .
FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive LLVM_VERSION=11

RUN apt-get update \
    && apt-get install ca-certificates lsb-release wget gnupg apt-transport-https \
        --yes --no-install-recommends --verbose-versions \
    && export LLVM_PUBKEY_HASH="bda960a8da687a275a2078d43c111d66b1c6a893a3275271beedf266c1ff4a0cdecb429c7a5cccf9f486ea7aa43fd27f" \
    && wget -nv -O /tmp/llvm-snapshot.gpg.key https://apt.llvm.org/llvm-snapshot.gpg.key \
    && echo "${LLVM_PUBKEY_HASH} /tmp/llvm-snapshot.gpg.key" | sha384sum -c \
    && apt-key add /tmp/llvm-snapshot.gpg.key \
    && export CODENAME="$(lsb_release --codename --short | tr 'A-Z' 'a-z')" \
    && echo "deb [trusted=yes] http://apt.llvm.org/${CODENAME}/ llvm-toolchain-${CODENAME}-${LLVM_VERSION} main" >> \
        /etc/apt/sources.list

# initial packages
RUN apt-get update \
    && apt-get install \
        bash \
        fakeroot \
        ccache \
        curl \
        software-properties-common \
        --yes --no-install-recommends

# Special dpkg-deb (https://github.com/ClickHouse-Extras/dpkg) version which is able
# to compress files using pigz (https://zlib.net/pigz/) instead of gzip.
# Significantly increase deb packaging speed and compatible with old systems
RUN curl -O https://clickhouse-builds.s3.yandex.net/utils/1/dpkg-deb \
    && chmod +x dpkg-deb \
    && cp dpkg-deb /usr/bin

# Libraries from OS are only needed to test the "unbundled" build (this is not used in production).
RUN apt-get update \
    && apt-get install \
        gcc-9 \
        g++-9 \
        clang-11 \
        clang-tidy-11 \
        lld-11 \
        llvm-11 \
        llvm-11-dev \
        clang-10 \
        clang-tidy-10 \
        lld-10 \
        llvm-10 \
        llvm-10-dev \
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
        moreutils \
        pigz \
        --yes --no-install-recommends

# NOTE: For some reason we have outdated version of gcc-10 in ubuntu 20.04 stable.
# Current workaround is to use latest version proposed repo. Remove as soon as
# gcc-10.2 appear in stable repo.
RUN echo 'deb http://archive.ubuntu.com/ubuntu/ focal-proposed restricted main multiverse universe' > /etc/apt/sources.list.d/proposed-repositories.list

RUN apt-get update \
    && apt-get install gcc-10 g++-10 --yes --no-install-recommends

RUN rm /etc/apt/sources.list.d/proposed-repositories.list && apt-get update

# This symlink required by gcc to find lld compiler
RUN ln -s /usr/bin/lld-${LLVM_VERSION} /usr/bin/ld.lld

COPY build.sh /

CMD ["/bin/bash", "/build.sh"]
