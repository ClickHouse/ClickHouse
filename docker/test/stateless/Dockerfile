# rebuild in #33610
# docker build -t clickhouse/stateless-test .
ARG FROM_TAG=latest
FROM clickhouse/test-base:$FROM_TAG

ARG odbc_driver_url="https://github.com/ClickHouse/clickhouse-odbc/releases/download/v1.1.4.20200302/clickhouse-odbc-1.1.4-Linux.tar.gz"

# golang version 1.13 on Ubuntu 20 is enough for tests
RUN apt-get update -y \
    && env DEBIAN_FRONTEND=noninteractive \
        apt-get install --yes --no-install-recommends \
            awscli \
            brotli \
            expect \
            golang \
            lsof \
            mysql-client=8.0* \
            ncdu \
            netcat-openbsd \
            nodejs \
            npm \
            openjdk-11-jre-headless \
            openssl \
            postgresql-client \
            protobuf-compiler \
            python3 \
            python3-lxml \
            python3-pip \
            python3-requests \
            python3-termcolor \
            qemu-user-static \
            sqlite3 \
            sudo \
            telnet \
            tree \
            unixodbc \
            wget \
            zstd \
            file \
            pv \
    && apt-get clean


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

# Unrelated to vars in setup_minio.sh, but should be the same there
# to have the same binaries for local running scenario
ARG MINIO_SERVER_VERSION=2022-01-03T18-22-58Z
ARG MINIO_CLIENT_VERSION=2022-01-05T23-52-51Z
ARG TARGETARCH

# Download Minio-related binaries
RUN arch=${TARGETARCH:-amd64} \
    && wget "https://dl.min.io/server/minio/release/linux-${arch}/archive/minio.RELEASE.${MINIO_SERVER_VERSION}" -O ./minio \
    && wget "https://dl.min.io/client/mc/release/linux-${arch}/archive/mc.RELEASE.${MINIO_CLIENT_VERSION}" -O ./mc \
    && chmod +x ./mc ./minio


RUN wget 'https://dlcdn.apache.org/hadoop/common/hadoop-3.3.1/hadoop-3.3.1.tar.gz' \
    && tar -xvf hadoop-3.3.1.tar.gz \
    && rm -rf hadoop-3.3.1.tar.gz

ENV MINIO_ROOT_USER="clickhouse"
ENV MINIO_ROOT_PASSWORD="clickhouse"
ENV EXPORT_S3_STORAGE_POLICIES=1

RUN npm install -g azurite

COPY run.sh /
COPY setup_minio.sh /
COPY setup_hdfs_minicluster.sh /
CMD ["/bin/bash", "/run.sh"]
