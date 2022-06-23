#  docker build -t clickhouse/fasttest .
FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive LLVM_VERSION=11

RUN sed -i 's|http://archive|http://ru.archive|g' /etc/apt/sources.list

RUN apt-get update \
    && apt-get install ca-certificates lsb-release wget gnupg apt-transport-https \
        --yes --no-install-recommends --verbose-versions \
    && export LLVM_PUBKEY_HASH="bda960a8da687a275a2078d43c111d66b1c6a893a3275271beedf266c1ff4a0cdecb429c7a5cccf9f486ea7aa43fd27f" \
    && wget -nv -O /tmp/llvm-snapshot.gpg.key https://apt.llvm.org/llvm-snapshot.gpg.key \
    && echo "${LLVM_PUBKEY_HASH} /tmp/llvm-snapshot.gpg.key" | sha384sum -c \
    && apt-key add /tmp/llvm-snapshot.gpg.key \
    && export CODENAME="$(lsb_release --codename --short | tr 'A-Z' 'a-z')" \
    && echo "deb [trusted=yes] https://apt.llvm.org/${CODENAME}/ llvm-toolchain-${CODENAME}-${LLVM_VERSION} main" >> \
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
RUN curl -O https://clickhouse-datasets.s3.yandex.net/utils/1/dpkg-deb \
    && chmod +x dpkg-deb \
    && cp dpkg-deb /usr/bin

RUN apt-get update \
    && apt-get install \
        apt-transport-https \
        bash \
        brotli \
        build-essential \
        ca-certificates \
        ccache \
        clang-${LLVM_VERSION} \
        clang-tidy-${LLVM_VERSION} \
        cmake \
        curl \
        expect \
        fakeroot \
        gdb \
        git \
        gperf \
        lld-${LLVM_VERSION} \
        llvm-${LLVM_VERSION} \
        lsof \
        moreutils \
        ninja-build \
        psmisc \
        python3 \
        python3-lxml \
        python3-pip \
        python3-requests \
        python3-termcolor \
        rename \
        software-properties-common \
        tzdata \
        unixodbc \
       --yes --no-install-recommends

RUN pip3 install numpy scipy pandas

# This symlink required by gcc to find lld compiler
RUN ln -s /usr/bin/lld-${LLVM_VERSION} /usr/bin/ld.lld

ARG odbc_driver_url="https://github.com/ClickHouse/clickhouse-odbc/releases/download/v1.1.4.20200302/clickhouse-odbc-1.1.4-Linux.tar.gz"

RUN mkdir -p /tmp/clickhouse-odbc-tmp \
  && wget -nv -O - ${odbc_driver_url} | tar --strip-components=1 -xz -C /tmp/clickhouse-odbc-tmp \
  && cp /tmp/clickhouse-odbc-tmp/lib64/*.so /usr/local/lib/ \
  && odbcinst -i -d -f /tmp/clickhouse-odbc-tmp/share/doc/clickhouse-odbc/config/odbcinst.ini.sample \
  && odbcinst -i -s -l -f /tmp/clickhouse-odbc-tmp/share/doc/clickhouse-odbc/config/odbc.ini.sample \
  && rm -rf /tmp/clickhouse-odbc-tmp

ENV TZ=Europe/Moscow
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

ENV COMMIT_SHA=''
ENV PULL_REQUEST_NUMBER=''
ENV COPY_CLICKHOUSE_BINARY_TO_OUTPUT=0

COPY run.sh /
CMD ["/bin/bash", "/run.sh"]
