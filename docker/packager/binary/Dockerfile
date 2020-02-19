# docker build -t yandex/clickhouse-binary-builder .
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

RUN apt-get update -y \
    && env DEBIAN_FRONTEND=noninteractive \
        apt-get install --yes --no-install-recommends \
            bash \
            cmake \
            ccache \
            curl \
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
            ninja-build \
            gperf \
            git \
            tzdata \
            gperf \
            cmake \
            gdb \
            rename \
            wget

RUN ln -s /usr/bin/lld-8 /usr/bin/ld.lld

ENV CC=clang-8
ENV CXX=clang++-8

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
RUN wget "https://developer.arm.com/-/media/Files/downloads/gnu-a/8.3-2019.03/binrel/gcc-arm-8.3-2019.03-x86_64-aarch64-linux-gnu.tar.xz?revision=2e88a73f-d233-4f96-b1f4-d8b36e9bb0b9&la=en" -O gcc-arm-8.3-2019.03-x86_64-aarch64-linux-gnu.tar.xz

COPY build.sh /
CMD ["/bin/bash", "/build.sh"]
