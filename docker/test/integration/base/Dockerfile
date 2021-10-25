# docker build -t clickhouse/integration-test .
FROM clickhouse/test-base

SHELL ["/bin/bash", "-c"]

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
        iproute2 \
        lsof \
        g++ \
        default-jre

RUN rm -rf \
        /var/lib/apt/lists/* \
        /var/cache/debconf \
        /tmp/* \
RUN apt-get clean

# Install MySQL ODBC driver
RUN curl 'https://downloads.mysql.com/archives/get/p/10/file/mysql-connector-odbc-8.0.21-linux-glibc2.12-x86-64bit.tar.gz' --location --output 'mysql-connector.tar.gz' && tar -xzf mysql-connector.tar.gz && cd mysql-connector-odbc-8.0.21-linux-glibc2.12-x86-64bit/lib && mv * /usr/local/lib && ln -s /usr/local/lib/libmyodbc8a.so /usr/lib/x86_64-linux-gnu/odbc/libmyodbc.so

# Unfortunately this is required for a single test for conversion data from zookeeper to clickhouse-keeper.
# ZooKeeper is not started by default, but consumes some space in containers.
# 777 perms used to allow anybody to start/stop ZooKeeper
ENV ZOOKEEPER_VERSION='3.6.3'
RUN curl -O "https://mirrors.estointernet.in/apache/zookeeper/zookeeper-${ZOOKEEPER_VERSION}/apache-zookeeper-${ZOOKEEPER_VERSION}-bin.tar.gz"
RUN tar -zxvf apache-zookeeper-${ZOOKEEPER_VERSION}-bin.tar.gz && mv apache-zookeeper-${ZOOKEEPER_VERSION}-bin /opt/zookeeper && chmod -R 777 /opt/zookeeper && rm apache-zookeeper-${ZOOKEEPER_VERSION}-bin.tar.gz
RUN echo $'tickTime=2500 \n\
tickTime=2500 \n\
dataDir=/zookeeper \n\
clientPort=2181 \n\
maxClientCnxns=80' > /opt/zookeeper/conf/zoo.cfg
RUN mkdir /zookeeper && chmod -R 777 /zookeeper

ENV TZ=Europe/Moscow
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
