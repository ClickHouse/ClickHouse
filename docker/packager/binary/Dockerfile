# Trigger new image build
# docker build -t yandex/clickhouse-binary-builder .
FROM ubuntu:19.10

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
            tzdata \
            gperf \
            cmake \
            gdb \
            rename \
            wget \
            build-essential

# This symlink required by gcc to find lld compiler
RUN ln -s /usr/bin/lld-9 /usr/bin/ld.lld

ENV CC=clang-9
ENV CXX=clang++-9

# libtapi is required to support .tbh format from recent MacOS SDKs
RUN git clone https://github.com/tpoechtrager/apple-libtapi.git
RUN cd apple-libtapi && INSTALLPREFIX=/cctools ./build.sh && ./install.sh
RUN rm -rf apple-libtapi

# Build and install tools for cross-linking to Darwin
RUN git clone https://github.com/tpoechtrager/cctools-port.git
RUN cd cctools-port/cctools && ./configure --prefix=/cctools --with-libtapi=/cctools --target=x86_64-apple-darwin && make install
RUN rm -rf cctools-port

# Download toolchain for Darwin
RUN wget https://github.com/phracker/MacOSX-SDKs/releases/download/10.14-beta4/MacOSX10.14.sdk.tar.xz

# Download toolchain for ARM
# It contains all required headers and libraries. Note that it's named as "gcc" but actually we are using clang for cross compiling.
RUN wget "https://developer.arm.com/-/media/Files/downloads/gnu-a/8.3-2019.03/binrel/gcc-arm-8.3-2019.03-x86_64-aarch64-linux-gnu.tar.xz?revision=2e88a73f-d233-4f96-b1f4-d8b36e9bb0b9&la=en" -O gcc-arm-8.3-2019.03-x86_64-aarch64-linux-gnu.tar.xz

COPY build.sh /
CMD ["/bin/bash", "/build.sh"]
