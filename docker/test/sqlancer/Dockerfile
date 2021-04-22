# docker build -t yandex/clickhouse-sqlancer-test .
FROM ubuntu:20.04

RUN apt-get update --yes && env DEBIAN_FRONTEND=noninteractive apt-get install wget unzip git openjdk-14-jdk maven --yes --no-install-recommends

RUN wget https://github.com/sqlancer/sqlancer/archive/master.zip -O /sqlancer.zip
RUN mkdir /sqlancer && \
	cd /sqlancer && \
	unzip /sqlancer.zip
RUN cd /sqlancer/sqlancer-master && mvn package -DskipTests

COPY run.sh /
CMD ["/bin/bash", "/run.sh"]
