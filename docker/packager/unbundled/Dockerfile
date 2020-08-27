# docker build -t yandex/clickhouse-unbundled-builder .
FROM yandex/clickhouse-deb-builder

ENV APACHE_PUBKEY_HASH="ec52a838a734b9d5e09d95c0167b09bf1af9565f814d4d5b3785f66fd5b53dd01c918d9ec008cb58e843324dec523d26"

RUN  export CODENAME="$(lsb_release --codename --short | tr 'A-Z' 'a-z')" \
    && wget -O /tmp/arrow-keyring.deb "https://apache.bintray.com/arrow/ubuntu/apache-arrow-archive-keyring-latest-${CODENAME}.deb" \
    && echo "${APACHE_PUBKEY_HASH} /tmp/arrow-keyring.deb" | sha384sum -c \
    && dpkg -i /tmp/arrow-keyring.deb

# Libraries from OS are only needed to test the "unbundled" build (that is not used in production).
RUN apt-get update \
    && apt-get install \
        libicu-dev \
        gperf \
        perl \
        pkg-config \
        devscripts \
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

COPY build.sh /

CMD ["/bin/bash", "/build.sh"]
