# docker build --network=host -t clickhouse/keeper-persistent-watcher .
# ZooKeeper Java client docker container

FROM ubuntu:22.04

RUN apt-get update \
    && apt-get install -y software-properties-common build-essential openjdk-17-jdk curl tar \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*

RUN curl -L -o /zookeeper-jute-3.6.3.jar https://repo1.maven.org/maven2/org/apache/zookeeper/zookeeper-jute/3.6.3/zookeeper-jute-3.6.3.jar && \
    curl -L -o /zookeeper-3.6.3.jar https://repo1.maven.org/maven2/org/apache/zookeeper/zookeeper/3.6.3/zookeeper-3.6.3.jar

ENV CLASSPATH=$CLASSPATH:/zookeeper-jute-3.6.3.jar
ENV CLASSPATH=$CLASSPATH:/zookeeper-3.6.3.jar

WORKDIR /jdbc

COPY ZooKeeperWatcher.java .
COPY ZooKeeperRecursiveWatcher.java .

RUN javac ZooKeeperWatcher.java && \
    javac ZooKeeperRecursiveWatcher.java
