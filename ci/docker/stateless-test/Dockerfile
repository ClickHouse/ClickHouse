# docker build -t clickhouse/stateless-test .
FROM ubuntu:22.04

# ARG for quick switch to a given ubuntu mirror
ARG apt_archive="https://archive.ubuntu.com"
RUN sed -i -e "s|http://archive.ubuntu.com|$apt_archive|g" -e "s|https://archive.ubuntu.com|$apt_archive|g" /etc/apt/sources.list

ARG odbc_driver_url="https://github.com/ClickHouse/clickhouse-odbc/releases/download/v1.1.6.20200320/clickhouse-odbc-1.1.6-Linux.tar.gz"

ARG LLVM_APT_VERSION="1:19.1.4"

RUN mkdir /etc/clickhouse-server /etc/clickhouse-keeper /etc/clickhouse-client /usr/share/clickhouse/ && chmod 777 /etc/clickhouse-* /usr/share/clickhouse \
    && mkdir -p /var/lib/clickhouse /var/log/clickhouse-server && chmod 777 /var/log/clickhouse-server /var/lib/clickhouse

RUN addgroup --gid 1000 clickhouse && adduser --uid 1000 --gid 1000 --disabled-password clickhouse
RUN addgroup --gid 1001 clickhouse2 && adduser --uid 1001 --gid 1001 --disabled-password clickhouse2

ENV DEBIAN_FRONTEND=noninteractive LLVM_VERSION=19

# moreutils - provides ts fo FT
# expect, bzip2 - requried by FT
# bsdmainutils - provides hexdump for FT
# git - required in one of FT TC
# gnuplot - for perf tests
# imagemagick - for perf tests
# parallel - for perf tests

# We need ca-certificates first to be able to update all repos. This is why it's run twice
RUN apt-get update \
    && apt-get install ca-certificates --yes --no-install-recommends --verbose-versions

# golang version 1.13 on Ubuntu 20 is enough for tests
RUN apt-get update -y \
    && apt-get install --yes --no-install-recommends \
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
        gnupg \
        lsb-release \
    && export LLVM_PUBKEY_HASH="bda960a8da687a275a2078d43c111d66b1c6a893a3275271beedf266c1ff4a0cdecb429c7a5cccf9f486ea7aa43fd27f" \
    && wget -nv -O /tmp/llvm-snapshot.gpg.key https://apt.llvm.org/llvm-snapshot.gpg.key \
    && echo "${LLVM_PUBKEY_HASH} /tmp/llvm-snapshot.gpg.key" | sha384sum -c \
    && apt-key add /tmp/llvm-snapshot.gpg.key \
    && export CODENAME="$(lsb_release --codename --short | tr 'A-Z' 'a-z')" \
    && echo "deb https://apt.llvm.org/${CODENAME}/ llvm-toolchain-${CODENAME}-${LLVM_VERSION} main" >> /etc/apt/sources.list.d/llvm.list \
    && apt-get update \
    && apt-get satisfy --yes --no-install-recommends "llvm-${LLVM_VERSION} (>= ${LLVM_APT_VERSION})" \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*
        
# Sanitizer options for services (clickhouse-server)
# Set resident memory limit for TSAN to 45GiB (46080MiB) to avoid OOMs in Stress tests
# and MEMORY_LIMIT_EXCEEDED exceptions in Functional tests (total memory limit in Functional tests is ~55.24 GiB).
# TSAN will flush shadow memory when reaching this limit.
# It may cause false-negatives, but it's better than OOM.
#  max_allocation_size_mb is set to 32GB, so we have much bigger chance to run into memory limit than the limitation of the sanitizers
RUN echo "TSAN_OPTIONS='verbosity=1000 halt_on_error=1 abort_on_error=1 history_size=7 memory_limit_mb=46080 second_deadlock_stack=1 max_allocation_size_mb=32768'" >> /etc/environment
RUN echo "UBSAN_OPTIONS='print_stacktrace=1 max_allocation_size_mb=32768'" >> /etc/environment
RUN echo "MSAN_OPTIONS='abort_on_error=1 poison_in_dtor=1 max_allocation_size_mb=32768'" >> /etc/environment
RUN echo "LSAN_OPTIONS='suppressions=/usr/share/clickhouse-test/config/lsan_suppressions.txt max_allocation_size_mb=32768'" >> /etc/environment
RUN echo "ASAN_OPTIONS='halt_on_error=1 abort_on_error=1'" >> /etc/environment
# Sanitizer options for current shell (not current, but the one that will be spawned on "docker run")
# (but w/o verbosity for TSAN, otherwise test.reference will not match)
ENV TSAN_OPTIONS='halt_on_error=1 abort_on_error=1 history_size=7 memory_limit_mb=46080 second_deadlock_stack=1 max_allocation_size_mb=32768'
ENV UBSAN_OPTIONS='print_stacktrace=1 max_allocation_size_mb=32768'
ENV MSAN_OPTIONS='abort_on_error=1 poison_in_dtor=1 max_allocation_size_mb=32768'
ENV LSAN_OPTIONS='max_allocation_size_mb=32768'
ENV ASAN_OPTIONS='halt_on_error=1 abort_on_error=1'

# for external_symbolizer_path, and also ensure that llvm-symbolizer really
# exists (since you don't want to fallback to addr2line, it is very slow)
RUN test -f /usr/bin/llvm-symbolizer-${LLVM_VERSION}
RUN ln -s /usr/bin/llvm-symbolizer-${LLVM_VERSION} /usr/bin/llvm-symbolizer

RUN BRIDGE_VERSION='25.1.5.31' \
    && ARCH=$(dpkg --print-architecture) \
    && cd /tmp \
    && wget "https://packages.clickhouse.com/deb/pool/main/c/clickhouse/clickhouse-odbc-bridge_${BRIDGE_VERSION}_${ARCH}.deb" \
    && wget "https://packages.clickhouse.com/deb/pool/main/c/clickhouse/clickhouse-library-bridge_${BRIDGE_VERSION}_${ARCH}.deb" \
    && dpkg -i clickhouse-odbc-bridge_${BRIDGE_VERSION}_${ARCH}.deb clickhouse-library-bridge_${BRIDGE_VERSION}_${ARCH}.deb \
    && rm clickhouse-odbc-bridge_${BRIDGE_VERSION}_${ARCH}.deb clickhouse-library-bridge_${BRIDGE_VERSION}_${ARCH}.deb

# Install Node.js 20 from NodeSource (azurite 3.34 fails with 24)
RUN curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y --no-install-recommends nodejs \
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

RUN npm install -g azurite@3.34.0 \
    && npm install -g tslib

ENV PYTHONPATH=".:./ci"

#USER clickhouse

# A directory for cache
RUN mkdir /dev/shm/clickhouse

COPY --from=clickhouse/cctools:4670e95dde3de689f103 /opt/gdb /opt/gdb
ENV PATH="/opt/gdb/bin:${PATH}"
