# docker build -t clickhouse/sqlancer-test .
FROM ubuntu:20.04

# ARG for quick switch to a given ubuntu mirror
ARG apt_archive="http://archive.ubuntu.com"
RUN sed -i "s|http://archive.ubuntu.com|$apt_archive|g" /etc/apt/sources.list

RUN apt-get update --yes && env DEBIAN_FRONTEND=noninteractive apt-get install wget unzip git default-jdk maven python3 --yes --no-install-recommends
RUN wget https://github.com/sqlancer/sqlancer/archive/master.zip -O /sqlancer.zip
RUN mkdir /sqlancer && \
	cd /sqlancer && \
	unzip /sqlancer.zip
RUN cd /sqlancer/sqlancer-master && mvn package -DskipTests

COPY run.sh /
COPY process_sqlancer_result.py /
CMD ["/bin/bash", "/run.sh"]
