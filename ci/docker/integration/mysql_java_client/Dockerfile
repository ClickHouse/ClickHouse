# docker build -t clickhouse/mysql-java-client .
# MySQL Java client docker container
# Rebuild to enable SBOM/provenance attestations (see #97903, #98511)

FROM eclipse-temurin:21-jdk-alpine

RUN apk --no-cache add curl

# Connector/J 8.2.0+ (including 9.x) reads the `info` field of the MySQL `OK`
# packet as a length-encoded string; it used to be pinned to 8.1.0 because
# ClickHouse wrote that field as a plain string and newer drivers failed to
# connect. Fixed by length-encoding `info` (see #84542), so test against a
# modern driver. CLASSPATH=. is required so javac-compiled .class files in the
# WORKDIR are found at runtime.
ARG ver=9.7.0
RUN curl -L -o /mysql-connector-j-${ver}.jar https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/${ver}/mysql-connector-j-${ver}.jar
ENV CLASSPATH=.:/mysql-connector-j-${ver}.jar

WORKDIR /jdbc
COPY MySQLJavaClientTest.java MySQLJavaClientTest.java
RUN javac MySQLJavaClientTest.java
