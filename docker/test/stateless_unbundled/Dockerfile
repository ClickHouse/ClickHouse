# docker build -t clickhouse/stateless-unbundled-test .
FROM clickhouse/test-base

ARG odbc_driver_url="https://github.com/ClickHouse/clickhouse-odbc/releases/download/v1.1.4.20200302/clickhouse-odbc-1.1.4-Linux.tar.gz"

RUN apt-get --allow-unauthenticated update -y \
    && env DEBIAN_FRONTEND=noninteractive \
        apt-get --allow-unauthenticated install --yes --no-install-recommends \
            alien \
            brotli \
            zstd \
            cmake \
            devscripts \
            expect \
            gdb  \
            gperf \
            heimdal-multidev \
            libboost-filesystem-dev \
            libboost-iostreams-dev \
            libboost-program-options-dev \
            libboost-regex-dev \
            libboost-system-dev \
            libboost-thread-dev \
            libc++-dev \
            libc++abi-dev \
            libcapnp-dev \
            libcctz-dev \
            libcurl4-openssl-dev \
            libdouble-conversion-dev \
            libgoogle-perftools-dev \
            libhyperscan-dev \
            libicu-dev \
            libjemalloc-dev \
            libldap2-dev \
            libltdl-dev \
            liblz4-dev \
            libmsgpack-dev \
            libpoco-dev \
            libpoconetssl62 \
            librdkafka-dev \
            libre2-dev \
            libreadline-dev \
            libsasl2-dev \
            libzstd-dev \
            librocksdb-dev \
            libgflags-dev \
            lsof \
            moreutils \
            ncdu \
            netcat-openbsd \
            odbcinst \
            openssl \
            perl \
            pigz \
            pkg-config \
            python3 \
            python3-lxml \
            python3-requests \
            python3-termcolor \
            python3-pip \
            qemu-user-static \
            sudo \
            telnet \
            tree \
            tzdata \
            unixodbc \
            unixodbc-dev \
            wget \
            zlib1g-dev

RUN pip3 install numpy scipy pandas

RUN mkdir -p /tmp/clickhouse-odbc-tmp \
   && wget -nv -O - ${odbc_driver_url} | tar --strip-components=1 -xz -C /tmp/clickhouse-odbc-tmp \
   && cp /tmp/clickhouse-odbc-tmp/lib64/*.so /usr/local/lib/ \
   && odbcinst -i -d -f /tmp/clickhouse-odbc-tmp/share/doc/clickhouse-odbc/config/odbcinst.ini.sample \
   && odbcinst -i -s -l -f /tmp/clickhouse-odbc-tmp/share/doc/clickhouse-odbc/config/odbc.ini.sample \
   && rm -rf /tmp/clickhouse-odbc-tmp

ENV TZ=Europe/Moscow
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

COPY run.sh /
CMD ["/bin/bash", "/run.sh"]

