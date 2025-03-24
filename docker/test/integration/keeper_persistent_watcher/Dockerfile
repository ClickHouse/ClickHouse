# docker build --network=host -t clickhouse/keeper-persistent-watcher .
# ZooKeeper Java client docker container

FROM ubuntu:22.04

RUN apt-get update \
    && apt-get install -y software-properties-common build-essential openjdk-17-jdk curl tar \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*

RUN curl -L -o /zookeeper-jute-3.6.3.jar https://repo1.maven.org/maven2/org/apache/zookeeper/zookeeper-jute/3.6.3/zookeeper-jute-3.6.3.jar && \
    curl -L -o /zookeeper-3.6.3.jar https://repo1.maven.org/maven2/org/apache/zookeeper/zookeeper/3.6.3/zookeeper-3.6.3.jar && \
    curl -L -o /slf4j-api-1.7.32.jar https://repo1.maven.org/maven2/org/slf4j/slf4j-api/1.7.32/slf4j-api-1.7.32.jar && \
    curl -L -o /slf4j-log4j12-1.7.32.jar https://repo1.maven.org/maven2/org/slf4j/slf4j-log4j12/1.7.32/slf4j-log4j12-1.7.32.jar && \
    curl -L -o /log4j-1.2.17.jar https://repo1.maven.org/maven2/log4j/log4j/1.2.17/log4j-1.2.17.jar && \
    curl -L -o /jetty-server-9.4.44.v20210927.jar https://repo1.maven.org/maven2/org/eclipse/jetty/jetty-server/9.4.44.v20210927/jetty-server-9.4.44.v20210927.jar && \
    curl -L -o /jetty-servlet-9.4.44.v20210927.jar https://repo1.maven.org/maven2/org/eclipse/jetty/jetty-servlet/9.4.44.v20210927/jetty-servlet-9.4.44.v20210927.jar

ENV CLASSPATH=$CLASSPATH:/zookeeper-jute-3.6.3.jar
ENV CLASSPATH=$CLASSPATH:/zookeeper-3.6.3.jar
ENV CLASSPATH=$CLASSPATH:/slf4j-api-1.7.32.jar
ENV CLASSPATH=$CLASSPATH:/slf4j-log4j12-1.7.32.jar
ENV CLASSPATH=$CLASSPATH:/log4j-1.2.17.jar
ENV CLASSPATH=$CLASSPATH:/jetty-server-9.4.44.v20210927.jar
ENV CLASSPATH=$CLASSPATH:/jetty-servlet-9.4.44.v20210927.jar

WORKDIR /jdbc

COPY ZooKeeperWatcher.java .
COPY ZooKeeperRecursiveWatcher.java .

RUN echo "Current directory listing:" && ls -l

RUN javac ZooKeeperWatcher.java && \
    javac ZooKeeperRecursiveWatcher.java
