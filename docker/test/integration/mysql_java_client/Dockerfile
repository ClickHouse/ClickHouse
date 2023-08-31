# docker build -t clickhouse/mysql-java-client .
# MySQL Java client docker container

FROM ubuntu:22.04

RUN apt-get update && \
    apt-get install -y software-properties-common build-essential openjdk-8-jdk curl

RUN rm -rf \
        /var/lib/apt/lists/* \
        /var/cache/debconf \
        /tmp/* \
RUN apt-get clean

ARG ver=8.1.0
RUN curl -L -o /mysql-connector-j-${ver}.jar https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/${ver}/mysql-connector-j-${ver}.jar
ENV CLASSPATH=$CLASSPATH:/mysql-connector-j-${ver}.jar

WORKDIR /jdbc
COPY Test.java Test.java
COPY PreparedStatementsTest.java PreparedStatementsTest.java
RUN javac Test.java PreparedStatementsTest.java
