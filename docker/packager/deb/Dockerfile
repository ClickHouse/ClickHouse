FROM ubuntu:18.04

RUN echo "deb [trusted=yes] http://apt.llvm.org/bionic/ llvm-toolchain-bionic-7 main" >> /etc/apt/sources.list
RUN echo "deb [trusted=yes] http://apt.llvm.org/bionic/ llvm-toolchain-bionic-8 main" >> /etc/apt/sources.list

RUN apt-get --allow-unauthenticated update -y \
    && env DEBIAN_FRONTEND=noninteractive \
        apt-get --allow-unauthenticated install --yes --no-install-recommends \
            bash \
            fakeroot \
            cmake \
            ccache \
            curl \
            gcc-7 \
            g++-7 \
            gcc-8 \
            g++-8 \
            clang-6.0 \
            lld-6.0 \
            libclang-6.0-dev \
            liblld-6.0-dev \
            llvm-6.0 \
            libllvm6.0 \
            llvm-6.0-dev \
            clang-7 \
            lld-7 \
            libclang-7-dev \
            liblld-7-dev \
            clang-8 \
            lld-8 \
            libclang-8-dev \
            liblld-8-dev \
            libicu-dev \
            libreadline-dev \
            gperf \
            ninja-build \
            perl \
            pkg-config \
            devscripts \
            debhelper \
            git \
            libc++-dev \
            libc++abi-dev \
            libboost-program-options-dev \
            libboost-system-dev \
            libboost-filesystem-dev \
            libboost-thread-dev \
            zlib1g-dev \
            liblz4-dev \
            libdouble-conversion-dev \
            libsparsehash-dev \
            librdkafka-dev \
            libpoconetssl50 \
            libpoco-dev \
            libsparsehash-dev \
            libgoogle-perftools-dev \
            libzstd-dev \
            libltdl-dev \
            libre2-dev \
            libjemalloc-dev \
            unixodbc-dev \
            odbcinst \
            tzdata

COPY build.sh /
CMD ["/bin/bash", "/build.sh"]
