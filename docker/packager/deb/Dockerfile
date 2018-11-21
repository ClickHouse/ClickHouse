FROM ubuntu:18.10

RUN apt-get update -y \
    && env DEBIAN_FRONTEND=noninteractive \
        apt-get install --yes --no-install-recommends \
            bash \
            fakeroot \
            cmake \
            curl \
            gcc-7 \
            g++-7 \
            gcc-8 \
            g++-8 \
            clang-6.0 \
            clang-7 \
            lld-6.0 \
            libclang-6.0-dev \
            liblld-6.0-dev \
            llvm-6.0 \
            libllvm6.0 \
            llvm-6.0-dev \
            lld-7 \
            libclang-7-dev \
            liblld-7-dev \
            libllvm7 \
            llvm-7-dev \
            libicu-dev \
            libreadline-dev \
            ninja-build \
            perl \
            pkg-config \
            devscripts \
            debhelper \
            git \
            libc++abi-dev \
            libc++-dev \
            libboost-program-options-dev \
            libboost-system-dev \
            libboost-filesystem-dev \
            libboost-thread-dev \
            zlib1g-dev \
            liblz4-dev \
            libdouble-conversion-dev \
            libsparsehash-dev \
            librdkafka-dev \
            libpoconetssl60 \
            libpoco-dev \
            libsparsehash-dev \
            libgoogle-perftools-dev \
            libzstd-dev \
            libre2-dev \
            libjemalloc-dev \
            unixodbc-dev \
            odbcinst


CMD /bin/bash build/release --no-pbuilder && mv /*.deb /output
