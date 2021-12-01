# docker build -t clickhouse/stateless-test .
FROM clickhouse/test-base

ARG odbc_driver_url="https://github.com/ClickHouse/clickhouse-odbc/releases/download/v1.1.4.20200302/clickhouse-odbc-1.1.4-Linux.tar.gz"

RUN echo "deb [trusted=yes] http://repo.mysql.com/apt/ubuntu/ bionic mysql-5.7" >> /etc/apt/sources.list \
    && apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 8C718D3B5072E1F5

RUN apt-get update -y \
    && env DEBIAN_FRONTEND=noninteractive \
        apt-get install --yes --no-install-recommends \
            brotli \
            expect \
            zstd \
            lsof \
            ncdu \
            netcat-openbsd \
            openssl \
            protobuf-compiler \
            python3 \
            python3-lxml \
            python3-requests \
            python3-termcolor \
            python3-pip \
            qemu-user-static \
            sudo \
            # golang version 1.13 on Ubuntu 20 is enough for tests
            golang \
            telnet \
            tree \
            unixodbc \
            wget \
            mysql-client=5.7* \
            postgresql-client \
            sqlite3

RUN pip3 install numpy scipy pandas Jinja2

RUN mkdir -p /tmp/clickhouse-odbc-tmp \
   && wget -nv -O - ${odbc_driver_url} | tar --strip-components=1 -xz -C /tmp/clickhouse-odbc-tmp \
   && cp /tmp/clickhouse-odbc-tmp/lib64/*.so /usr/local/lib/ \
   && odbcinst -i -d -f /tmp/clickhouse-odbc-tmp/share/doc/clickhouse-odbc/config/odbcinst.ini.sample \
   && odbcinst -i -s -l -f /tmp/clickhouse-odbc-tmp/share/doc/clickhouse-odbc/config/odbc.ini.sample \
   && rm -rf /tmp/clickhouse-odbc-tmp

ENV TZ=Europe/Moscow
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

ENV NUM_TRIES=1
ENV MAX_RUN_TIME=0


# Download Minio-related binaries
RUN wget 'https://dl.min.io/server/minio/release/linux-amd64/minio' \
    && chmod +x ./minio \
    && wget 'https://dl.min.io/client/mc/release/linux-amd64/mc' \
    && chmod +x ./mc

ENV MINIO_ROOT_USER="clickhouse"
ENV MINIO_ROOT_PASSWORD="clickhouse"

COPY run.sh /
COPY setup_minio.sh /
CMD ["/bin/bash", "/run.sh"]
