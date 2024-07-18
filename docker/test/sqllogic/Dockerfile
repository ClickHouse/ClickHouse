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
    && apt-get clean

RUN pip3 install \
    numpy \
    pyodbc \
    deepdiff

ARG odbc_repo="https://github.com/ClickHouse/clickhouse-odbc.git"

RUN git clone --recursive ${odbc_repo} \
    && mkdir -p /clickhouse-odbc/build \
    && cmake -S /clickhouse-odbc -B /clickhouse-odbc/build \
    && ls /clickhouse-odbc/build/driver \
    && make -j 10 -C /clickhouse-odbc/build \
    && ls /clickhouse-odbc/build/driver \
    && mkdir -p /usr/local/lib64/ && cp /clickhouse-odbc/build/driver/lib*.so /usr/local/lib64/ \
    && odbcinst -i -d -f /clickhouse-odbc/packaging/odbcinst.ini.sample \
    && odbcinst -i -s -l -f /clickhouse-odbc/packaging/odbc.ini.sample

ENV TZ=Europe/Amsterdam
ENV MAX_RUN_TIME=900
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

ARG sqllogic_test_repo="https://github.com/gregrahn/sqllogictest.git"

RUN git clone --recursive ${sqllogic_test_repo}

COPY run.sh /
CMD ["/bin/bash", "/run.sh"]
