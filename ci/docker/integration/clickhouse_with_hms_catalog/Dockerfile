#  docker build -t clickhouse/integration-test-with-hms .
ARG FROM_TAG=latest
FROM eclipse-temurin:8-jre AS build

RUN apt-get update && apt-get -o Acquire::Retries=5 install --fix-missing -y curl

ENV HADOOP_VERSION=3.3.6
ENV AWS_SDK_BUNDLE=1.12.753

RUN curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar -Lo /tmp/hadoop-aws-${HADOOP_VERSION}.jar
RUN curl https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE}.jar -Lo /tmp/aws-java-sdk-bundle-${AWS_SDK_BUNDLE}.jar

FROM maven:3.9-eclipse-temurin-11 AS paimon-build
WORKDIR /app
COPY paimon-rest-catalog/ .
RUN mvn clean package -DskipTests -B

FROM maven:3.9-eclipse-temurin-11 AS incremental-writer-build
WORKDIR /app
COPY paimon-incremental-data/ .
RUN mvn clean package -DskipTests -B

FROM apache/hive:4.0.0

ENV HADOOP_VERSION=3.3.6
ENV AWS_SDK_BUNDLE=1.12.753

COPY --from=build /tmp/hadoop-aws-${HADOOP_VERSION}.jar /opt/hive/lib/hadoop-aws-${HADOOP_VERSION}.jar
COPY --from=build /tmp/aws-java-sdk-bundle-${AWS_SDK_BUNDLE}.jar /opt/hive/lib/aws-java-sdk-bundle-${AWS_SDK_BUNDLE}.jar
COPY core-site.xml /opt/hadoop/etc/hadoop/core-site.xml
COPY --from=paimon-build /app/target/paimon-server-starter-1.0-SNAPSHOT.jar /opt/paimon/paimon-server.jar
COPY --from=incremental-writer-build /app/target/paimon-incremental-writer-1.1.1.jar /opt/paimon/paimon-incremental-writer.jar
