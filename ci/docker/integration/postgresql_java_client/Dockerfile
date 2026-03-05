# docker build -t clickhouse/postgresql-java-client .
# PostgreSQL Java client docker container

FROM ubuntu:24.04

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-21-jdk curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*

ARG ver=42.7.7
RUN curl -L -o /postgresql-java-${ver}.jar https://repo1.maven.org/maven2/org/postgresql/postgresql/${ver}/postgresql-${ver}.jar
ENV CLASSPATH=.:/postgresql-java-${ver}.jar

WORKDIR /jdbc
COPY Test.java Test.java
RUN javac Test.java
