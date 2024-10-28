# docker build -t clickhouse/fasttest .
FROM ubuntu:22.04

# ARG for quick switch to a given ubuntu mirror
ARG apt_archive="http://archive.ubuntu.com"
RUN sed -i "s|http://archive.ubuntu.com|$apt_archive|g" /etc/apt/sources.list

ENV DEBIAN_FRONTEND=noninteractive LLVM_VERSION=18

RUN apt-get update \
    && apt-get install \
        apt-transport-https \
        apt-utils \
        ca-certificates \
        curl \
        gnupg \
        lsb-release \
        wget \
        git \
        --yes --no-install-recommends --verbose-versions \
    && export LLVM_PUBKEY_HASH="bda960a8da687a275a2078d43c111d66b1c6a893a3275271beedf266c1ff4a0cdecb429c7a5cccf9f486ea7aa43fd27f" \
    && wget -nv -O /tmp/llvm-snapshot.gpg.key https://apt.llvm.org/llvm-snapshot.gpg.key \
    && echo "${LLVM_PUBKEY_HASH} /tmp/llvm-snapshot.gpg.key" | sha384sum -c \
    && apt-key add /tmp/llvm-snapshot.gpg.key \
    && export CODENAME="$(lsb_release --codename --short | tr 'A-Z' 'a-z')" \
    && echo "deb https://apt.llvm.org/${CODENAME}/ llvm-toolchain-${CODENAME}-${LLVM_VERSION} main" >> \
        /etc/apt/sources.list \
    && apt-get update \
    && apt-get install --yes --no-install-recommends --verbose-versions llvm-${LLVM_VERSION} \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*

# moreutils - provides ts fo FT
# expect, bzip2 - requried by FT
# bsdmainutils - provides hexdump for FT
# nasm - nasm copiler for one of submodules, required from normal build
# yasm - asssembler for libhdfs3, required from normal build

RUN apt-get update \
    && apt-get install \
        clang-${LLVM_VERSION} \
        cmake \
        libclang-${LLVM_VERSION}-dev \
        libclang-rt-${LLVM_VERSION}-dev \
        lld-${LLVM_VERSION} \
        llvm-${LLVM_VERSION}-dev \
        lsof \
        ninja-build \
        python3 \
        python3-pip \
        zstd \
        moreutils \
        expect \
        bsdmainutils \
        pv \
        jq \
        bzip2 \
        nasm \
        yasm \
        --yes --no-install-recommends \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*

COPY --from=clickhouse/cctools:0d6b90a7a490 /opt/gdb /opt/gdb
# Give suid to gdb to grant it attach permissions
RUN chmod u+s /opt/gdb/bin/gdb
ENV PATH="/opt/gdb/bin:${PATH}"

# This symlink is required by gcc to find the lld linker
RUN ln -s /usr/bin/lld-${LLVM_VERSION} /usr/bin/ld.lld
# FIXME: workaround for "The imported target "merge-fdata" references the file" error
# https://salsa.debian.org/pkg-llvm-team/llvm-toolchain/-/commit/992e52c0b156a5ba9c6a8a54f8c4857ddd3d371d
RUN sed -i '/_IMPORT_CHECK_FILES_FOR_\(mlir-\|llvm-bolt\|merge-fdata\|MLIR\)/ {s|^|#|}' /usr/lib/llvm-${LLVM_VERSION}/lib/cmake/llvm/LLVMExports-*.cmake

# LLVM changes paths for compiler-rt libraries. For some reason clang-18.1.8 cannot catch up libraries from default install path.
# It's very dirty workaround, better to build compiler and LLVM ourself and use it. Details: https://github.com/llvm/llvm-project/issues/95792
RUN test ! -d /usr/lib/llvm-18/lib/clang/18/lib/x86_64-pc-linux-gnu || ln -s /usr/lib/llvm-18/lib/clang/18/lib/x86_64-pc-linux-gnu /usr/lib/llvm-18/lib/clang/18/lib/x86_64-unknown-linux-gnu

ARG TARGETARCH
ARG SCCACHE_VERSION=v0.7.7
ENV SCCACHE_IGNORE_SERVER_IO_ERROR=1
# sccache requires a value for the region. So by default we use The Default Region
ENV SCCACHE_REGION=us-east-1
RUN arch=${TARGETARCH} \
  && case $arch in \
    amd64) rarch=x86_64 ;; \
    arm64) rarch=aarch64 ;; \
  esac \
  && curl -Ls "https://github.com/mozilla/sccache/releases/download/$SCCACHE_VERSION/sccache-$SCCACHE_VERSION-$rarch-unknown-linux-musl.tar.gz" | \
    tar xz -C /tmp \
  && mv "/tmp/sccache-$SCCACHE_VERSION-$rarch-unknown-linux-musl/sccache" /usr/bin \
  && rm "/tmp/sccache-$SCCACHE_VERSION-$rarch-unknown-linux-musl" -r

COPY requirements.txt /
RUN pip3 install --no-cache-dir -r /requirements.txt

# chmod 777 to make the container user independent
RUN mkdir -p /var/lib/clickhouse \
  && chmod 777 /var/lib/clickhouse

ENV TZ=Europe/Amsterdam
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN groupadd --system --gid 1000 clickhouse \
    && useradd --system --gid 1000 --uid 1000 -m clickhouse \
    && mkdir -p /.cache/sccache && chmod 777 /.cache/sccache

ENV PYTHONPATH="/wd"
ENV PYTHONUNBUFFERED=1
