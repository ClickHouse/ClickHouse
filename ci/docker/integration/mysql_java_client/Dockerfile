# docker build -t clickhouse/mysql-java-client .
# MySQL Java client docker container

FROM eclipse-temurin:21-jdk-alpine

RUN apk --no-cache add curl

# Pin to 8.1.0 (original version): Connector/J 8.2.0+ and 9.x introduce breaking
# changes to the ClickHouse MySQL protocol handshake. CLASSPATH=. is required so
# javac-compiled .class files in the WORKDIR are found at runtime.
ARG ver=8.1.0
RUN curl -L -o /mysql-connector-j-${ver}.jar https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/${ver}/mysql-connector-j-${ver}.jar
ENV CLASSPATH=.:/mysql-connector-j-${ver}.jar

WORKDIR /jdbc
COPY MySQLJavaClientTest.java MySQLJavaClientTest.java
RUN javac MySQLJavaClientTest.java
