# rebuild in #33610
# docker build -t clickhouse/deb-builder .
FROM ubuntu:20.04

# ARG for quick switch to a given ubuntu mirror
ARG apt_archive="http://archive.ubuntu.com"
RUN sed -i "s|http://archive.ubuntu.com|$apt_archive|g" /etc/apt/sources.list

ENV DEBIAN_FRONTEND=noninteractive LLVM_VERSION=13

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

# Architecture of the image when BuildKit/buildx is used
ARG TARGETARCH

# Special dpkg-deb (https://github.com/ClickHouse-Extras/dpkg) version which is able
# to compress files using pigz (https://zlib.net/pigz/) instead of gzip.
# Significantly increase deb packaging speed and compatible with old systems
RUN arch=${TARGETARCH:-amd64} \
  && curl -Lo /usr/bin/dpkg-deb https://github.com/ClickHouse-Extras/dpkg/releases/download/1.21.1-clickhouse/dpkg-deb-${arch}

RUN apt-get update \
    && apt-get install \
        alien \
        clang-${LLVM_VERSION} \
        clang-tidy-${LLVM_VERSION} \
        cmake \
        debhelper \
        devscripts \
        gdb  \
        git \
        gperf \
        lld-${LLVM_VERSION} \
        llvm-${LLVM_VERSION} \
        llvm-${LLVM_VERSION}-dev \
        moreutils \
        ninja-build \
        perl \
        pigz \
        pixz \
        pkg-config \
        tzdata \
        --yes --no-install-recommends

# NOTE: Seems like gcc-11 is too new for ubuntu20 repository
RUN add-apt-repository ppa:ubuntu-toolchain-r/test --yes \
    && apt-get update \
    && apt-get install gcc-11 g++-11 --yes


# These symlinks are required:
# /usr/bin/ld.lld: by gcc to find lld compiler
# /usr/bin/aarch64-linux-gnu-obj*: for debug symbols stripping
RUN ln -s /usr/bin/lld-${LLVM_VERSION} /usr/bin/ld.lld \
    && ln -sf /usr/lib/llvm-${LLVM_VERSION}/bin/llvm-objcopy /usr/bin/aarch64-linux-gnu-strip \
    && ln -sf /usr/lib/llvm-${LLVM_VERSION}/bin/llvm-objcopy /usr/bin/aarch64-linux-gnu-objcopy \
    && ln -sf /usr/lib/llvm-${LLVM_VERSION}/bin/llvm-objdump /usr/bin/aarch64-linux-gnu-objdump


COPY build.sh /

CMD ["/bin/bash", "/build.sh"]

