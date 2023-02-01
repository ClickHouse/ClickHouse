# docker build -t clickhouse/test-util .
FROM ubuntu:20.04

# ARG for quick switch to a given ubuntu mirror
ARG apt_archive="http://archive.ubuntu.com"
RUN sed -i "s|http://archive.ubuntu.com|$apt_archive|g" /etc/apt/sources.list

ENV DEBIAN_FRONTEND=noninteractive LLVM_VERSION=14

RUN apt-get update \
    && apt-get install \
        apt-transport-https \
        apt-utils \
        ca-certificates \
        dnsutils \
        gnupg \
        iputils-ping \
        lsb-release \
        wget \
        --yes --no-install-recommends --verbose-versions \
    && export LLVM_PUBKEY_HASH="bda960a8da687a275a2078d43c111d66b1c6a893a3275271beedf266c1ff4a0cdecb429c7a5cccf9f486ea7aa43fd27f" \
    && wget -nv -O /tmp/llvm-snapshot.gpg.key https://apt.llvm.org/llvm-snapshot.gpg.key \
    && echo "${LLVM_PUBKEY_HASH} /tmp/llvm-snapshot.gpg.key" | sha384sum -c \
    && apt-key add /tmp/llvm-snapshot.gpg.key \
    && export CODENAME="$(lsb_release --codename --short | tr 'A-Z' 'a-z')" \
    && echo "deb [trusted=yes] https://apt.llvm.org/${CODENAME}/ llvm-toolchain-${CODENAME}-${LLVM_VERSION} main" >> \
        /etc/apt/sources.list \
    && apt-get clean

# initial packages
RUN apt-get update \
    && apt-get install \
        bash \
        bsdmainutils \
        build-essential \
        clang-${LLVM_VERSION} \
        clang-tidy-${LLVM_VERSION} \
        cmake \
        curl \
        fakeroot \
        gdb \
        git \
        gperf \
        lld-${LLVM_VERSION} \
        llvm-${LLVM_VERSION} \
        llvm-${LLVM_VERSION}-dev \
        moreutils \
        nasm \
        ninja-build \
        pigz \
        rename \
        software-properties-common \
        tzdata \
        --yes --no-install-recommends \
    && apt-get clean

# This symlink required by gcc to find lld compiler
RUN ln -s /usr/bin/lld-${LLVM_VERSION} /usr/bin/ld.lld

ARG CCACHE_VERSION=4.6.1
RUN mkdir /tmp/ccache \
    && cd /tmp/ccache \
    && curl -L \
        -O https://github.com/ccache/ccache/releases/download/v$CCACHE_VERSION/ccache-$CCACHE_VERSION.tar.xz \
        -O https://github.com/ccache/ccache/releases/download/v$CCACHE_VERSION/ccache-$CCACHE_VERSION.tar.xz.asc \
    && gpg --recv-keys --keyserver hkps://keyserver.ubuntu.com 5A939A71A46792CF57866A51996DDA075594ADB8 \
    && gpg --verify ccache-4.6.1.tar.xz.asc \
    && tar xf ccache-$CCACHE_VERSION.tar.xz \
    && cd /tmp/ccache/ccache-$CCACHE_VERSION \
    && cmake -DCMAKE_INSTALL_PREFIX=/usr \
        -DCMAKE_BUILD_TYPE=None \
        -DZSTD_FROM_INTERNET=ON \
        -DREDIS_STORAGE_BACKEND=OFF \
        -Wno-dev \
        -B build \
        -S . \
    && make VERBOSE=1 -C build \
    && make install -C build \
    && cd / \
    && rm -rf /tmp/ccache

COPY process_functional_tests_result.py /
