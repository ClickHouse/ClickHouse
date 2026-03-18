# docker build -t clickhouse/integration-test .
FROM ubuntu:22.04

# ARG for quick switch to a given ubuntu mirror
ARG apt_archive="http://archive.ubuntu.com"
RUN sed -i "s|http://archive.ubuntu.com|$apt_archive|g" /etc/apt/sources.list

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install \
        sudo \
        apt-transport-https \
        apt-utils \
        ca-certificates \
        curl \
        dnsutils \
        gnupg \
        iputils-ping \
        lsb-release \
#        wget \
        --yes --no-install-recommends --verbose-versions \
#    && export LLVM_PUBKEY_HASH="bda960a8da687a275a2078d43c111d66b1c6a893a3275271beedf266c1ff4a0cdecb429c7a5cccf9f486ea7aa43fd27f" \
#    && wget -nv -O /tmp/llvm-snapshot.gpg.key https://apt.llvm.org/llvm-snapshot.gpg.key \
#    && echo "${LLVM_PUBKEY_HASH} /tmp/llvm-snapshot.gpg.key" | sha384sum -c \
#    && apt-key add /tmp/llvm-snapshot.gpg.key \
#    && export CODENAME="$(lsb_release --codename --short | tr 'A-Z' 'a-z')" \
#    && echo "deb https://apt.llvm.org/${CODENAME}/ llvm-toolchain-${CODENAME}-${LLVM_VERSION} main" >> \
#        /etc/apt/sources.list \
#    && apt-get update \
#    && apt-get satisfy --yes --no-install-recommends "llvm-${LLVM_VERSION} (>= ${LLVM_APT_VERSION})" \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*

SHELL ["/bin/bash", "-c"]

RUN apt-get update \
    && env DEBIAN_FRONTEND=noninteractive apt-get -y install \
        bsdutils \
        default-jre \
        g++ \
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
        python3-pip \
        libcurl4-openssl-dev \
        libssl-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*

COPY requirements.txt /
RUN pip3 install --no-cache-dir -r requirements.txt && rm -rf /root/.cache/pip

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
  && curl -o mysql-odbc.rpm "https://cdn.mysql.com/archives/mysql-connector-odbc-8.0/mysql-connector-odbc-8.0.32-1.el9.${rarch}.rpm" \
  && rpm2archive mysql-odbc.rpm \
  && tar xf mysql-odbc.rpm.tgz -C / ./usr/lib64/ \
  && rm mysql-odbc.rpm mysql-odbc.rpm.tgz \
  && ODBC_DIR=$(dpkg -L odbc-postgresql | grep '^/usr/lib/.*-linux-gnu/odbc$') \
  && ln -s /usr/lib64/libmyodbc8a.so "$ODBC_DIR" \
  && ln -s /usr/lib64/libmyodbc8a.so "$ODBC_DIR"/libmyodbc.so

# Unfortunately this is required for a single test for conversion data from zookeeper to clickhouse-keeper.
# ZooKeeper is not started by default, but consumes some space in containers.
# 777 perms used to allow anybody to start/stop ZooKeeper
ENV ZOOKEEPER_VERSION='3.6.3'
RUN curl "https://archive.apache.org/dist/zookeeper/zookeeper-${ZOOKEEPER_VERSION}/apache-zookeeper-${ZOOKEEPER_VERSION}-bin.tar.gz" | \
      tar -C opt -zxv && \
  mv /opt/apache-zookeeper-${ZOOKEEPER_VERSION}-bin /opt/zookeeper && \
  chmod -R 777 /opt/zookeeper && \
  echo $'tickTime=2500 \n\
tickTime=2500 \n\
dataDir=/zookeeper \n\
clientPort=2181 \n\
maxClientCnxns=80' > /opt/zookeeper/conf/zoo.cfg && \
  mkdir /zookeeper && \
  chmod -R 777 /zookeeper

ENV TZ=Etc/UTC
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
