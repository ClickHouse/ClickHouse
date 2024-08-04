# docker build -t clickhouse/test-util .
FROM ubuntu:22.04

# ARG for quick switch to a given ubuntu mirror
ARG apt_archive="http://archive.ubuntu.com"
RUN sed -i "s|http://archive.ubuntu.com|$apt_archive|g" /etc/apt/sources.list

ENV DEBIAN_FRONTEND=noninteractive LLVM_VERSION=18 LLVM_COMMIT=3b5b5c1ec4a3095ab096dd780e84d7ab81f3d7ff

# initial packages
RUN apt-get update \
    && apt-get install \
        sudo \
        apt-transport-https \
        apt-utils \
        ca-certificates \
        curl \
        dnsutils \
        gnupg \
        iputils-ping \
        lsb-release \
        wget \
        bash \
        bsdmainutils \
        gdb \
        gperf \
        moreutils \
        nasm \
        xz-utils \
        bzip2 \
        pigz \
        rename \
        software-properties-common \
        tzdata \
        git \
        ninja-build \
        clang \
        libzstd-dev \
        zlib1g-dev \
        --yes --no-install-recommends --verbose-versions \
        && apt-get clean && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*

# Install cmake 3.20+ for Rust support
# Used https://askubuntu.com/a/1157132 as reference

# Install LLVM by compiling from sources, using a particular commit.
# This is more robust. See https://github.com/llvm/llvm-project/issues/99453
# Feel free to update LLVM_COMMIT if desired.

RUN curl -s https://apt.kitware.com/keys/kitware-archive-latest.asc | gpg --dearmor - > /etc/apt/trusted.gpg.d/kitware.gpg \
    && echo "deb https://apt.kitware.com/ubuntu/ $(lsb_release -cs) main" >> /etc/apt/sources.list \
    && apt-get update && apt-get install --yes cmake \
    && apt-get clean && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/* \
    && git clone --no-checkout https://github.com/llvm/llvm-project \
    && cd llvm-project && git checkout ${LLVM_COMMIT} && cd .. \
    && mkdir llvm-build && cd llvm-build \
    && CC=clang-14 CXX=clang++-14 cmake -GNinja -DCMAKE_BUILD_TYPE:STRING=Release -DLLVM_ENABLE_ZLIB=1 -DLLVM_ENABLE_PROJECTS=all -DLLVM_TARGETS_TO_BUILD=all ../llvm-project/llvm \
    && ninja && ninja install \
    && cd .. && rm -rf llvm-project llvm-build \
    && apt-get remove --yes gcc clang \
    && apt-get clean && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*

# Create symlinks for tools
RUN ln -f -s /usr/local/bin/clang++ /usr/local/bin/clang++-${LLVM_VERSION} \
    && ln -f -s /usr/local/bin/ld.lld /usr/local/bin/ld.lld-${LLVM_VERSION} \
    && ln -f -s /usr/local/bin/llvm-as /usr/local/bin/llvm-as-${LLVM_VERSION} \
    && ln -f -s /usr/local/bin/llvm-ar /usr/local/bin/llvm-ar-${LLVM_VERSION} \
    && ln -f -s /usr/local/bin/llvm-ranlib /usr/local/bin/llvm-ranlib-${LLVM_VERSION} \
    && ln -f -s /usr/local/bin/llvm-nm /usr/local/bin/llvm-nm-${LLVM_VERSION} \
    && ln -f -s /usr/local/bin/llvm-install-name-tool /usr/local/bin/llvm-install-name-tool-${LLVM_VERSION} \
    && ln -f -s /usr/local/bin/llvm-objcopy /usr/local/bin/llvm-objcopy-${LLVM_VERSION} \
    && ln -f -s /usr/local/bin/llvm-strip /usr/local/bin/llvm-strip-${LLVM_VERSION}

COPY process_functional_tests_result.py /
