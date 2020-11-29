# docker build -t yandex/clickhouse-stateless-test-with-coverage .
# TODO: that can be based on yandex/clickhouse-stateless-test (llvm version and CMD differs)
FROM yandex/clickhouse-test-base

ARG odbc_driver_url="https://github.com/ClickHouse/clickhouse-odbc/releases/download/v1.1.4.20200302/clickhouse-odbc-1.1.4-Linux.tar.gz"

RUN apt-get update -y \
    && env DEBIAN_FRONTEND=noninteractive \
        apt-get install --yes --no-install-recommends \
            bash \
            tzdata \
            fakeroot \
            debhelper \
            expect \
            python3 \
            python3-lxml \
            python3-termcolor \
            python3-requests \
            sudo \
            openssl \
            ncdu \
            netcat-openbsd \
            telnet \
            tree \
            moreutils \
            brotli \
            zstd \
            gdb \
            lsof \
            unixodbc \
            wget \
            qemu-user-static \
            procps \
            psmisc

RUN mkdir -p /tmp/clickhouse-odbc-tmp \
   && wget -nv -O - ${odbc_driver_url} | tar --strip-components=1 -xz -C /tmp/clickhouse-odbc-tmp \
   && cp /tmp/clickhouse-odbc-tmp/lib64/*.so /usr/local/lib/ \
   && odbcinst -i -d -f /tmp/clickhouse-odbc-tmp/share/doc/clickhouse-odbc/config/odbcinst.ini.sample \
   && odbcinst -i -s -l -f /tmp/clickhouse-odbc-tmp/share/doc/clickhouse-odbc/config/odbc.ini.sample \
   && rm -rf /tmp/clickhouse-odbc-tmp

ENV TZ=Europe/Moscow
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
COPY run.sh /run.sh

CMD ["/bin/bash", "/run.sh"]
