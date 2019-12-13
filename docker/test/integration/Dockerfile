# docker build -t yandex/clickhouse-integration-test .
FROM ubuntu:18.04

RUN echo "deb [trusted=yes] http://apt.llvm.org/bionic/ llvm-toolchain-bionic-8 main" >> /etc/apt/sources.list

RUN apt-get update \
    && env DEBIAN_FRONTEND=noninteractive apt-get -y install \
        tzdata \
        python \
        libreadline-dev \
        libicu-dev \
        bsdutils \
        llvm-8 \
        gdb \
        unixodbc \
        odbcinst \
        libsqlite3-dev \
        libsqliteodbc \
        odbc-postgresql \
        sqlite3 \
        curl \
        tar \
        tzdata
RUN rm -rf \
        /var/lib/apt/lists/* \
        /var/cache/debconf \
        /tmp/* \
RUN apt-get clean

# Install MySQL ODBC driver
RUN curl 'https://cdn.mysql.com//Downloads/Connector-ODBC/8.0/mysql-connector-odbc-8.0.18-linux-glibc2.12-x86-64bit.tar.gz' --output 'mysql-connector.tar.gz' && tar -xzf mysql-connector.tar.gz && cd mysql-connector-odbc-8.0.18-linux-glibc2.12-x86-64bit/lib && mv * /usr/local/lib && ln -s /usr/local/lib/libmyodbc8a.so /usr/lib/x86_64-linux-gnu/odbc/libmyodbc.so

ENV TZ=Europe/Moscow
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN echo "TSAN_OPTIONS='halt_on_error=1 history_size=7'" >> /etc/environment; \
    echo "UBSAN_OPTIONS='print_stacktrace=1'" >> /etc/environment; \
    ln -s /usr/lib/llvm-8/bin/llvm-symbolizer /usr/bin/llvm-symbolizer;
