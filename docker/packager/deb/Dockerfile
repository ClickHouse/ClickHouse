# docker build -t yandex/clickhouse-deb-builder .
FROM ubuntu:19.10

ENV DEBIAN_FRONTEND=noninteractive

RUN \
    apt update && \
    apt install ca-certificates lsb-release wget gnupg apt-transport-https --yes --no-install-recommends --verbose-versions  && \
    wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add - && \
    export CODENAME="$(lsb_release --codename --short | tr 'A-Z' 'a-z')" && \
    echo "deb [trusted=yes] http://apt.llvm.org/${CODENAME}/ llvm-toolchain-${CODENAME}-10 main" >> /etc/apt/sources.list && \
    wget "https://apache.bintray.com/arrow/ubuntu/apache-arrow-archive-keyring-latest-${CODENAME}.deb" && \
    apt install "./apache-arrow-archive-keyring-latest-${CODENAME}.deb" --yes --no-install-recommends --verbose-versions && \
    rm -v "./apache-arrow-archive-keyring-latest-${CODENAME}.deb"

# initial packages
RUN apt update && \
    apt install --no-install-recommends \
            bash \
            fakeroot \
            ccache \
            curl \
            software-properties-common \
            --yes

# Special dpkg-deb (https://github.com/ClickHouse-Extras/dpkg) version which is able
# to compress files using pigz (https://zlib.net/pigz/) instead of gzip.
# Significantly increase deb packaging speed and compatible with old systems
RUN curl -O https://clickhouse-builds.s3.yandex.net/utils/1/dpkg-deb && \
    chmod +x dpkg-deb && \
    cp dpkg-deb /usr/bin


# Libraries from OS are only needed to test the "unbundled" build (this is not used in production).
RUN apt update && \
    apt install --no-install-recommends \
            gcc-9 \
            g++-9 \
            llvm-10 \
            llvm-10-dev \
            llvm-9-dev \
            clang-10 \
            lld-10 \
            clang-tidy-10 \
            clang-9 \
            lld-9 \
            clang-tidy-9 \
            libssl-dev \
            libicu-dev \
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
            libxml2-dev \
            librdkafka-dev \
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
            libgsasl7-dev \
            heimdal-multidev \
            libhyperscan-dev \
            libbrotli-dev \
            protobuf-compiler \
            libprotoc-dev \
            libgrpc++-dev \
            rapidjson-dev \
            libsnappy-dev \
            libparquet-dev \
            libthrift-dev \
            libavro-dev \
            libfarmhash-dev \
            libmysqlclient-dev \
            --yes


# This symlink required by gcc to find lld compiler
RUN ln -s /usr/bin/lld-10 /usr/bin/ld.lld

COPY build.sh /

CMD ["/bin/bash", "/build.sh"]
