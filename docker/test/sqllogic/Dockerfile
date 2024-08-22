# docker build -t clickhouse/sqllogic-test .
ARG FROM_TAG=latest
FROM clickhouse/test-base:$FROM_TAG

RUN apt-get update --yes \
    && env DEBIAN_FRONTEND=noninteractive \
        apt-get install --yes --no-install-recommends \
            wget \
            git \
            python3 \
            python3-dev \
            python3-pip \
            sqlite3 \
            unixodbc \
            unixodbc-dev \
            odbcinst \
            sudo \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*

COPY requirements.txt /
RUN pip3 install --no-cache-dir -r /requirements.txt

ARG odbc_driver_url="https://github.com/ClickHouse/clickhouse-odbc/releases/download/v1.1.6.20200320/clickhouse-odbc-1.1.6-Linux.tar.gz"

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

ARG sqllogic_test_repo="https://github.com/gregrahn/sqllogictest.git"

RUN git clone --recursive ${sqllogic_test_repo}

COPY run.sh /
CMD ["/bin/bash", "/run.sh"]
