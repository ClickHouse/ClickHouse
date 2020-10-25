# docker build -t yandex/clickhouse-integration-test .
FROM ubuntu:19.10

RUN apt-get update \
    && env DEBIAN_FRONTEND=noninteractive apt-get -y install \
        tzdata \
        python \
        libreadline-dev \
        libicu-dev \
        bsdutils \
        llvm-9 \
        gdb \
        unixodbc \
        odbcinst \
        libsqlite3-dev \
        libsqliteodbc \
        odbc-postgresql \
        sqlite3 \
        curl \
        tar
RUN rm -rf \
        /var/lib/apt/lists/* \
        /var/cache/debconf \
        /tmp/* \
RUN apt-get clean

# Install MySQL ODBC driver
RUN curl 'https://cdn.mysql.com//Downloads/Connector-ODBC/8.0/mysql-connector-odbc-8.0.21-linux-glibc2.12-x86-64bit.tar.gz' --output 'mysql-connector.tar.gz' && tar -xzf mysql-connector.tar.gz && cd mysql-connector-odbc-8.0.21-linux-glibc2.12-x86-64bit/lib && mv * /usr/local/lib && ln -s /usr/local/lib/libmyodbc8a.so /usr/lib/x86_64-linux-gnu/odbc/libmyodbc.so

ENV TZ=Europe/Moscow
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Sanitizer options
RUN echo "TSAN_OPTIONS='verbosity=1000 halt_on_error=1 history_size=7'" >> /etc/environment; \
    echo "UBSAN_OPTIONS='print_stacktrace=1'" >> /etc/environment; \
    echo "MSAN_OPTIONS='abort_on_error=1'" >> /etc/environment; \
    ln -s /usr/lib/llvm-9/bin/llvm-symbolizer /usr/bin/llvm-symbolizer;
