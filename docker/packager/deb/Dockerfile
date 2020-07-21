# docker build -t yandex/clickhouse-deb-builder .
FROM ubuntu:19.10

RUN apt-get --allow-unauthenticated update -y && apt-get install --yes wget gnupg
RUN wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add -
RUN echo "deb [trusted=yes] http://apt.llvm.org/eoan/ llvm-toolchain-eoan-10 main" >> /etc/apt/sources.list

# initial packages
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

# Special dpkg-deb (https://github.com/ClickHouse-Extras/dpkg) version which is able
# to compress files using pigz (https://zlib.net/pigz/) instead of gzip.
# Significantly increase deb packaging speed and compatible with old systems
RUN curl -O https://clickhouse-builds.s3.yandex.net/utils/dpkg-deb
RUN chmod +x dpkg-deb
RUN cp dpkg-deb /usr/bin


# Libraries from OS are only needed to test the "unbundled" build (that is not used in production).
RUN apt-get --allow-unauthenticated update -y \
    && env DEBIAN_FRONTEND=noninteractive \
        apt-get --allow-unauthenticated install --yes --no-install-recommends \
            gcc-9 \
            g++-9 \
            llvm-10 \
            clang-10 \
            lld-10 \
            clang-tidy-10 \
            clang-9 \
            lld-9 \
            clang-tidy-9 \
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
            libboost-iostreams-dev \
            libboost-regex-dev \
            zlib1g-dev \
            liblz4-dev \
            libdouble-conversion-dev \
            librdkafka-dev \
            libpoconetssl62 \
            libpoco-dev \
            libgoogle-perftools-dev \
            libzstd-dev \
            libltdl-dev \
            libre2-dev \
            libjemalloc-dev \
            libmsgpack-dev \
            libcurl4-openssl-dev \
            opencl-headers \
            ocl-icd-libopencl1 \
            intel-opencl-icd \
            unixodbc-dev \
            odbcinst \
            tzdata \
            gperf \
            alien \
            libcapnp-dev \
            cmake \
            gdb  \
            pigz \
            moreutils \
            libcctz-dev \
            libldap2-dev \
            libsasl2-dev \
            heimdal-multidev \
            libhyperscan-dev


# This symlink required by gcc to find lld compiler
RUN ln -s /usr/bin/lld-10 /usr/bin/ld.lld

COPY build.sh /

CMD ["/bin/bash", "/build.sh"]
