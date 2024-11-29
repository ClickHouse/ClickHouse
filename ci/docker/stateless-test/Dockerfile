# docker build -t clickhouse/stateless-test .
FROM ubuntu:22.04

# ARG for quick switch to a given ubuntu mirror
ARG apt_archive="http://archive.ubuntu.com"
RUN sed -i "s|http://archive.ubuntu.com|$apt_archive|g" /etc/apt/sources.list

ARG odbc_driver_url="https://github.com/ClickHouse/clickhouse-odbc/releases/download/v1.1.6.20200320/clickhouse-odbc-1.1.6-Linux.tar.gz"


RUN mkdir /etc/clickhouse-server /etc/clickhouse-keeper /etc/clickhouse-client && chmod 777 /etc/clickhouse-* \
    && mkdir -p /var/lib/clickhouse /var/log/clickhouse-server && chmod 777 /var/log/clickhouse-server /var/lib/clickhouse

RUN addgroup --gid 1000 clickhouse && adduser --uid 1000 --gid 1000 --disabled-password clickhouse
RUN addgroup --gid 1001 clickhouse2 && adduser --uid 1001 --gid 1001 --disabled-password clickhouse2

# moreutils - provides ts fo FT
# expect, bzip2 - requried by FT
# bsdmainutils - provides hexdump for FT
# git - required in one of FT TC
# gnuplot - for perf tests
# imagemagick - for perf tests
# parallel - for perf tests
# time - GNU time for perf, as it's difficult to use built-in bash time from python

# golang version 1.13 on Ubuntu 20 is enough for tests
RUN apt-get update -y \
    && env DEBIAN_FRONTEND=noninteractive \
        apt-get install --yes --no-install-recommends \
            git \
            awscli \
            brotli \
            lz4 \
            expect \
            moreutils \
            bzip2 \
            bsdmainutils \
            golang \
            lsof \
            mysql-client=8.0* \
            ncdu \
            netcat-openbsd \
            nodejs \
            npm \
            odbcinst \
            openjdk-11-jre-headless \
            openssl \
            postgresql-client \
            python3 \
            python3-pip \
            qemu-user-static \
            sqlite3 \
            sudo \
            tree \
            unixodbc \
            rustc \
            cargo \
            zstd \
            file \
            jq \
            pv \
            zip \
            unzip \
            p7zip-full \
            curl \
            wget \
            xz-utils \
            ripgrep \
            gnuplot \
            imagemagick \
            parallel \
            time \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*

ARG PROTOC_VERSION=25.1
RUN curl -OL https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-x86_64.zip \
    && unzip protoc-${PROTOC_VERSION}-linux-x86_64.zip -d /usr/local \
    && rm protoc-${PROTOC_VERSION}-linux-x86_64.zip

COPY requirements.txt /
RUN pip3 install --no-cache-dir -r /requirements.txt

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

# Unrelated to vars in setup_minio.sh, but should be the same there
# to have the same binaries for local running scenario
ARG MINIO_SERVER_VERSION=2024-08-03T04-33-23Z
ARG MINIO_CLIENT_VERSION=2024-07-31T15-58-33Z
ARG TARGETARCH

# Download Minio-related binaries
RUN arch=${TARGETARCH:-amd64} \
    && curl -L "https://dl.min.io/server/minio/release/linux-${arch}/archive/minio.RELEASE.${MINIO_SERVER_VERSION}" -o /minio \
    && curl -L "https://dl.min.io/client/mc/release/linux-${arch}/archive/mc.RELEASE.${MINIO_CLIENT_VERSION}" -o /mc \
    && chmod +x /mc /minio

ENV MINIO_ROOT_USER="clickhouse"
ENV MINIO_ROOT_PASSWORD="clickhouse"

# for minio to work without root
RUN chmod 777 /home
ENV HOME="/home"
ENV TEMP_DIR="/tmp/praktika"
ENV PATH="/wd/tests:/tmp/praktika/input:$PATH"

RUN npm install -g azurite@3.30.0 \
    && npm install -g tslib && npm install -g node

ENV PYTHONPATH=".:./ci"
USER clickhouse
