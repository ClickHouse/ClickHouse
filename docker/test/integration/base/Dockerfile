# rebuild in #33610
# docker build -t clickhouse/integration-test .
ARG FROM_TAG=latest
FROM clickhouse/test-base:$FROM_TAG

SHELL ["/bin/bash", "-c"]

RUN apt-get update \
    && env DEBIAN_FRONTEND=noninteractive apt-get -y install \
        bsdutils \
        curl \
        default-jre \
        g++ \
        gdb \
        iproute2 \
        krb5-user \
        libicu-dev \
        libsqlite3-dev \
        libsqliteodbc \
        lsof \
        lz4 \
        odbc-postgresql \
        odbcinst \
        python3 \
        rpm2cpio \
        sqlite3 \
        tar \
        tzdata \
        unixodbc \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*

# Architecture of the image when BuildKit/buildx is used
ARG TARGETARCH

# Install MySQL ODBC driver from RHEL rpm
# For reference https://downloads.mysql.com/archives/c-odbc/ RHEL
RUN arch=${TARGETARCH:-amd64} \
  && case $arch in \
      amd64) rarch=x86_64 ;; \
      arm64) rarch=aarch64 ;; \
    esac \
  && cd /tmp \
  && curl -o mysql-odbc.rpm "https://cdn.mysql.com/archives/mysql-connector-odbc-8.0/mysql-connector-odbc-8.0.27-1.el8.${rarch}.rpm" \
  && rpm2archive mysql-odbc.rpm \
  && tar xf mysql-odbc.rpm.tgz -C / ./usr/lib64/ \
  && LINK_DIR=$(dpkg -L libodbc1 | grep '^/usr/lib/.*-linux-gnu/odbc$') \
  && ln -s /usr/lib64/libmyodbc8a.so "$LINK_DIR" \
  && ln -s /usr/lib64/libmyodbc8a.so "$LINK_DIR"/libmyodbc.so

# Unfortunately this is required for a single test for conversion data from zookeeper to clickhouse-keeper.
# ZooKeeper is not started by default, but consumes some space in containers.
# 777 perms used to allow anybody to start/stop ZooKeeper
ENV ZOOKEEPER_VERSION='3.6.3'
RUN curl -O "https://dlcdn.apache.org/zookeeper/zookeeper-${ZOOKEEPER_VERSION}/apache-zookeeper-${ZOOKEEPER_VERSION}-bin.tar.gz"
RUN tar -zxvf apache-zookeeper-${ZOOKEEPER_VERSION}-bin.tar.gz && mv apache-zookeeper-${ZOOKEEPER_VERSION}-bin /opt/zookeeper && chmod -R 777 /opt/zookeeper && rm apache-zookeeper-${ZOOKEEPER_VERSION}-bin.tar.gz
RUN echo $'tickTime=2500 \n\
tickTime=2500 \n\
dataDir=/zookeeper \n\
clientPort=2181 \n\
maxClientCnxns=80' > /opt/zookeeper/conf/zoo.cfg
RUN mkdir /zookeeper && chmod -R 777 /zookeeper

ENV TZ=Etc/UTC
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
