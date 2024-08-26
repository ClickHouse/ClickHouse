# rebuild in #33610
# docker build -t altinityinfra/stateless-test .
ARG FROM_TAG=latest
FROM altinityinfra/test-base:$FROM_TAG

ARG odbc_driver_url="https://github.com/ClickHouse/clickhouse-odbc/releases/download/v1.1.6.20200320/clickhouse-odbc-1.1.6-Linux.tar.gz"

# golang version 1.13 on Ubuntu 20 is enough for tests
RUN apt-get update -y \
    && env DEBIAN_FRONTEND=noninteractive \
        apt-get install --yes --no-install-recommends \
            awscli='1.22.*' \
            brotli='1.0.*' \
            lz4='1.9.*' \
            expect='5.45.*' \
            golang='2:1.18~*' \
            lsof='4.93.*' \
            mysql-client='8.0*' \
            ncdu='1.15.*' \
            netcat-openbsd='1.218-*' \
            nodejs='12.22.*' \
            npm='8.5.*' \
            odbcinst='2.3.*' \
            openjdk-11-jre-headless='11.0.*' \
            openssl='3.0.*' \
            postgresql-client='14+*' \
            protobuf-compiler='3.12.*' \
            python3='3.10.*' \
            python3-lxml='4.8.*' \
            python3-pip='22.0.*' \
            python3-requests='2.25.*' \
            python3-termcolor='1.1.*' \
            qemu-user-static='1:6.2*' \
            sqlite3='3.37.*' \
            sudo='1.9.*' \
            tree='2.0.*' \
            unixodbc='2.3.*' \
            wget='1.21.*' \
            rustc='1.*' \
            cargo='1.*' \
            zstd='1.4.*' \
            file='1:5.41-*' \
            jq='1.6-*' \
            pv='1.6.*' \
            zip='3.0-*' \
            p7zip-full='16.02*' \
            rpm2cpio='4.17.*' \
            cpio='2.13*' \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*

RUN pip3 install numpy~=1.26.3 scipy~=1.12.0 pandas~=1.5.3 Jinja2~=3.1.3 pyarrow~=15.0.0

RUN mkdir -p /tmp/clickhouse-odbc-tmp \
  && cd /tmp/clickhouse-odbc-tmp \
  && curl -L ${odbc_driver_url} | tar --strip-components=1 -xz clickhouse-odbc-1.1.6-Linux \
  && mkdir /usr/local/lib64 -p \
  && cp /tmp/clickhouse-odbc-tmp/lib64/*.so /usr/local/lib64/ \
  && odbcinst -i -d -f /tmp/clickhouse-odbc-tmp/share/doc/clickhouse-odbc/config/odbcinst.ini.sample \
  && odbcinst -i -s -l -f /tmp/clickhouse-odbc-tmp/share/doc/clickhouse-odbc/config/odbc.ini.sample \
  && sed -i 's"=libclickhouseodbc"=/usr/local/lib64/libclickhouseodbc"' /etc/odbcinst.ini \
  && rm -rf /tmp/clickhouse-odbc-tmp

ENV TZ=Europe/Amsterdam
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
    && curl -L "https://dl.min.io/server/minio/release/linux-${arch}/archive/minio.RELEASE.${MINIO_SERVER_VERSION}" -o ./minio \
    && curl -L "https://dl.min.io/client/mc/release/linux-${arch}/archive/mc.RELEASE.${MINIO_CLIENT_VERSION}" -o ./mc \
    && chmod +x ./mc ./minio

RUN curl -L --no-verbose -O 'https://archive.apache.org/dist/hadoop/common/hadoop-3.3.1/hadoop-3.3.1.tar.gz' \
    && tar -xvf hadoop-3.3.1.tar.gz \
    && rm -rf hadoop-3.3.1.tar.gz

ENV MINIO_ROOT_USER="clickhouse"
ENV MINIO_ROOT_PASSWORD="clickhouse"
ENV EXPORT_S3_STORAGE_POLICIES=1

RUN npm install -g azurite@3.28.0 \
    && npm install -g tslib@2.6.2

COPY run.sh /
COPY setup_minio.sh /
COPY setup_hdfs_minicluster.sh /
COPY attach_gdb.lib /
COPY utils.lib /

# We store stress_tests.lib in stateless image to avoid duplication of this file in stress and upgrade tests
COPY stress_tests.lib /

CMD ["/bin/bash", "/run.sh"]
