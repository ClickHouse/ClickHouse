# docker build -t yandex/clickhouse-unbundled-builder .
FROM yandex/clickhouse-deb-builder

# Libraries from OS are only needed to test the "unbundled" build (that is not used in production).
RUN apt-get --allow-unauthenticated update -y \
    && env DEBIAN_FRONTEND=noninteractive \
        apt-get --allow-unauthenticated install --yes --no-install-recommends \
            libicu-dev \
            libreadline-dev \
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

COPY build.sh

CMD ["/bin/bash", "/build.sh"]
