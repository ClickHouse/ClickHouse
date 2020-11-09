#  docker build -t yandex/clickhouse-binary-builder .
FROM ubuntu:19.10

RUN apt-get --allow-unauthenticated update -y && apt-get install --yes wget gnupg
RUN wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add -
RUN echo "deb [trusted=yes] http://apt.llvm.org/eoan/ llvm-toolchain-eoan-10 main" >> /etc/apt/sources.list

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

RUN apt-get update -y \
    && env DEBIAN_FRONTEND=noninteractive \
        apt-get install --yes --no-install-recommends \
            bash \
            cmake \
            ccache \
            curl \
            gcc-9 \
            g++-9 \
            llvm-10 \
            clang-10 \
            lld-10 \
            clang-tidy-10 \
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
            build-essential

# This symlink required by gcc to find lld compiler
RUN ln -s /usr/bin/lld-10 /usr/bin/ld.lld

ENV CC=clang-10
ENV CXX=clang++-10

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
RUN wget https://github.com/phracker/MacOSX-SDKs/releases/download/10.14-beta4/MacOSX10.14.sdk.tar.xz

# Download toolchain for ARM
# It contains all required headers and libraries. Note that it's named as "gcc" but actually we are using clang for cross compiling.
RUN wget "https://developer.arm.com/-/media/Files/downloads/gnu-a/8.3-2019.03/binrel/gcc-arm-8.3-2019.03-x86_64-aarch64-linux-gnu.tar.xz?revision=2e88a73f-d233-4f96-b1f4-d8b36e9bb0b9&la=en" -O gcc-arm-8.3-2019.03-x86_64-aarch64-linux-gnu.tar.xz

# Download toolchain for FreeBSD 11.3
RUN wget https://clickhouse-datasets.s3.yandex.net/toolchains/toolchains/freebsd-11.3-toolchain.tar.xz

COPY build.sh /
CMD ["/bin/bash", "/build.sh"]
