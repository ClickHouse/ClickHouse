#  docker build -t yandex/clickhouse-fasttest .
FROM ubuntu:19.10

ARG odbc_driver_url="https://github.com/ClickHouse/clickhouse-odbc/releases/download/v1.1.4.20200302/clickhouse-odbc-1.1.4-Linux.tar.gz"
ENV COMMIT_SHA=''
ENV PULL_REQUEST_NUMBER=''

RUN apt-get --allow-unauthenticated update -y && apt-get install --yes wget gnupg
RUN wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add -
RUN echo "deb [trusted=yes] http://apt.llvm.org/eoan/ llvm-toolchain-eoan-10 main" >> /etc/apt/sources.list


RUN apt-get --allow-unauthenticated update -y \
  && env DEBIAN_FRONTEND=noninteractive \
      apt-get --allow-unauthenticated install --yes --no-install-recommends \
          bash \
          fakeroot \
          ccache \
          software-properties-common \
          apt-transport-https \
          ca-certificates \
          wget \
          bash \
          fakeroot \
          cmake \
          ccache \
          llvm-10 \
          clang-10 \
          lld-10 \
          clang-tidy-10 \
          ninja-build \
          gperf \
          git \
          tzdata \
          gperf \
          rename \
          build-essential \
          expect \
          python \
          python-lxml \
          python-termcolor \
          python-requests \
          unixodbc \
          qemu-user-static \
          sudo \
          moreutils \
          curl \
          brotli

RUN mkdir -p /tmp/clickhouse-odbc-tmp \
  && wget --quiet -O - ${odbc_driver_url} | tar --strip-components=1 -xz -C /tmp/clickhouse-odbc-tmp \
  && cp /tmp/clickhouse-odbc-tmp/lib64/*.so /usr/local/lib/ \
  && odbcinst -i -d -f /tmp/clickhouse-odbc-tmp/share/doc/clickhouse-odbc/config/odbcinst.ini.sample \
  && odbcinst -i -s -l -f /tmp/clickhouse-odbc-tmp/share/doc/clickhouse-odbc/config/odbc.ini.sample \
  && rm -rf /tmp/clickhouse-odbc-tmp

# This symlink required by gcc to find lld compiler
RUN ln -s /usr/bin/lld-10 /usr/bin/ld.lld

ENV TZ=Europe/Moscow
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone


COPY run.sh /
CMD ["/bin/bash", "/run.sh"]
