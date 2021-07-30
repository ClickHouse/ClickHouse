#  docker build -t yandex/clickhouse-binary-builder .
FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive LLVM_VERSION=10

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
    && wget -nv -O /tmp/llvm-snapshot.gpg.key https://apt.llvm.org/llvm-snapshot.gpg.key \
    && export LLVM_PUBKEY_HASH="bda960a8da687a275a2078d43c111d66b1c6a893a3275271beedf266c1ff4a0cdecb429c7a5cccf9f486ea7aa43fd27f" \
    && echo "${LLVM_PUBKEY_HASH} /tmp/llvm-snapshot.gpg.key" | sha384sum -c \
    && apt-key add /tmp/llvm-snapshot.gpg.key \
    && export CODENAME="$(lsb_release --codename --short | tr 'A-Z' 'a-z')" \
    && echo "deb [trusted=yes] https://apt.llvm.org/${CODENAME}/ llvm-toolchain-${CODENAME}-${LLVM_VERSION} main" >> \
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

RUN cat /etc/resolv.conf \
    && apt-get update \
    && apt-get install \
        bash \
        cmake \
        ccache \
        curl \
        gcc-9 \
        g++-9 \
        llvm-${LLVM_VERSION} \
        clang-${LLVM_VERSION} \
        lld-${LLVM_VERSION} \
        clang-tidy-${LLVM_VERSION} \
        clang-9 \
        lld-9 \
        clang-tidy-9 \
        clang-8 \
        lld-8 \
        clang-tidy-8 \
        libicu-dev \
        libreadline-dev \
        ninja-build \
        gperf \
        git \
        opencl-headers \
        ocl-icd-libopencl1 \
        intel-opencl-icd \
        tzdata \
        gperf \
        cmake \
        gdb \
        rename \
        wget \
        build-essential \
        --yes --no-install-recommends

# This symlink required by gcc to find lld compiler
RUN ln -s /usr/bin/lld-${LLVM_VERSION} /usr/bin/ld.lld

ENV CC=clang-${LLVM_VERSION}
ENV CXX=clang++-${LLVM_VERSION}

# libtapi is required to support .tbh format from recent MacOS SDKs
RUN git clone https://github.com/tpoechtrager/apple-libtapi.git \
    && cd apple-libtapi \
    && INSTALLPREFIX=/cctools ./build.sh \
    && ./install.sh \
    && cd .. \
    && rm -rf apple-libtapi

# Build and install tools for cross-linking to Darwin
RUN git clone https://github.com/tpoechtrager/cctools-port.git \
    && cd cctools-port/cctools \
    && ./configure --prefix=/cctools --with-libtapi=/cctools \
        --target=x86_64-apple-darwin \
    && make install \
    && cd ../.. \
    && rm -rf cctools-port

# Download toolchain for Darwin
RUN wget --retry-connrefused --tries=10 https://github.com/phracker/MacOSX-SDKs/releases/download/10.14-beta4/MacOSX10.14.sdk.tar.xz

# Download toolchain for ARM
# It contains all required headers and libraries. Note that it's named as "gcc" but actually we are using clang for cross compiling.
RUN wget --retry-connrefused --tries=10 "https://developer.arm.com/-/media/Files/downloads/gnu-a/8.3-2019.03/binrel/gcc-arm-8.3-2019.03-x86_64-aarch64-linux-gnu.tar.xz?revision=2e88a73f-d233-4f96-b1f4-d8b36e9bb0b9&la=en" -O gcc-arm-8.3-2019.03-x86_64-aarch64-linux-gnu.tar.xz

# Download toolchain for FreeBSD 11.3
RUN wget --retry-connrefused --tries=10 https://clickhouse-datasets.s3.yandex.net/toolchains/toolchains/freebsd-11.3-toolchain.tar.xz

COPY build.sh /
CMD ["/bin/bash", "/build.sh"]
