# docker build -t yandex/clickhouse-deb-builder .
FROM ubuntu:19.10

ENV DEBIAN_FRONTEND=noninteractive LLVM_VERSION=10

RUN apt-get update \
    && apt-get install ca-certificates lsb-release wget gnupg apt-transport-https \
        --yes --no-install-recommends --verbose-versions \
    && export LLVM_PUBKEY_HASH="bda960a8da687a275a2078d43c111d66b1c6a893a3275271beedf266c1ff4a0cdecb429c7a5cccf9f486ea7aa43fd27f" \
    && wget -O /tmp/llvm-snapshot.gpg.key https://apt.llvm.org/llvm-snapshot.gpg.key \
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

# Special dpkg-deb (https://github.com/ClickHouse-Extras/dpkg) version which is able
# to compress files using pigz (https://zlib.net/pigz/) instead of gzip.
# Significantly increase deb packaging speed and compatible with old systems
RUN curl -O https://clickhouse-builds.s3.yandex.net/utils/1/dpkg-deb \
    && chmod +x dpkg-deb \
    && cp dpkg-deb /usr/bin

ENV APACHE_PUBKEY_HASH="ec52a838a734b9d5e09d95c0167b09bf1af9565f814d4d5b3785f66fd5b53dd01c918d9ec008cb58e843324dec523d26"

RUN  export CODENAME="$(lsb_release --codename --short | tr 'A-Z' 'a-z')" \
    && wget -O /tmp/arrow-keyring.deb "https://apache.bintray.com/arrow/ubuntu/apache-arrow-archive-keyring-latest-${CODENAME}.deb" \
    && echo "${APACHE_PUBKEY_HASH} /tmp/arrow-keyring.deb" | sha384sum -c \
    && dpkg -i /tmp/arrow-keyring.deb

# Libraries from OS are only needed to test the "unbundled" build (this is not used in production).
RUN apt-get update \
    && apt-get install \
        gcc-9 \
        g++-9 \
        llvm-${LLVM_VERSION} \
        llvm-9-dev \
        llvm-${LLVM_VERSION}-dev \
        lld-9 \
        lld-${LLVM_VERSION} \
        clang-tidy-${LLVM_VERSION} \
        clang-9 \
        clang-${LLVM_VERSION} \
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
        libutf8proc-dev \
        libbz2-dev \
        libavro-dev \
        libfarmhash-dev \
        libmysqlclient-dev \
        --yes --no-install-recommends

# This symlink required by gcc to find lld compiler
RUN ln -s /usr/bin/lld-${LLVM_VERSION} /usr/bin/ld.lld

COPY build.sh /

CMD ["/bin/bash", "/build.sh"]
