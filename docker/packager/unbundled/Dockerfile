# docker build -t clickhouse/unbundled-builder .
FROM clickhouse/deb-builder

RUN export CODENAME="$(lsb_release --codename --short | tr 'A-Z' 'a-z')" \
    && wget -nv -O /tmp/arrow-keyring.deb "https://apache.jfrog.io/artifactory/arrow/ubuntu/apache-arrow-apt-source-latest-${CODENAME}.deb" \
    && dpkg -i /tmp/arrow-keyring.deb

RUN sed -i 's|http://archive|http://ru.archive|g' /etc/apt/sources.list

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
        libboost-context-dev \
        libboost-coroutine-dev \
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
        protobuf-compiler-grpc \
        libc-ares-dev \
        rapidjson-dev \
        libsnappy-dev \
        libparquet-dev \
        libthrift-dev \
        libutf8proc-dev \
        libbz2-dev \
        libavro-dev \
        libfarmhash-dev \
        librocksdb-dev \
        libgflags-dev \
        libmysqlclient-dev \
        --yes --no-install-recommends

COPY build.sh /

CMD ["/bin/bash", "/build.sh"]
