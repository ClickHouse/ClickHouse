# docker build -t yandex/clickhouse-integration-test .
FROM yandex/clickhouse-test-base

RUN apt-get update \
    && env DEBIAN_FRONTEND=noninteractive apt-get -y install \
        tzdata \
        python3 \
        libreadline-dev \
        libicu-dev \
        bsdutils \
        gdb \
        unixodbc \
        odbcinst \
        libsqlite3-dev \
        libsqliteodbc \
        odbc-postgresql \
        sqlite3 \
        curl \
        tar \
        krb5-user \
        iproute2
RUN rm -rf \
        /var/lib/apt/lists/* \
        /var/cache/debconf \
        /tmp/* \
RUN apt-get clean

# Install MySQL ODBC driver
RUN curl 'https://cdn.mysql.com//Downloads/Connector-ODBC/8.0/mysql-connector-odbc-8.0.21-linux-glibc2.12-x86-64bit.tar.gz' --output 'mysql-connector.tar.gz' && tar -xzf mysql-connector.tar.gz && cd mysql-connector-odbc-8.0.21-linux-glibc2.12-x86-64bit/lib && mv * /usr/local/lib && ln -s /usr/local/lib/libmyodbc8a.so /usr/lib/x86_64-linux-gnu/odbc/libmyodbc.so

ENV TZ=Europe/Moscow
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
