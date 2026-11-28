# docker build -t clickhouse/sqlancer-test .
FROM ubuntu:22.04

# ARG for quick switch to a given ubuntu mirror
ARG apt_archive="http://archive.ubuntu.com"
RUN sed -i -e "s|http://archive.ubuntu.com|$apt_archive|g" -e "s|https://archive.ubuntu.com|$apt_archive|g" /etc/apt/sources.list

# We need ca-certificates first to be able to update all repos. This is why it's run twice
RUN apt-get update \
    && apt-get install ca-certificates --yes --no-install-recommends --verbose-versions

RUN apt-get update --yes \
    && env DEBIAN_FRONTEND=noninteractive apt-get install wget git python3 default-jdk maven --yes --no-install-recommends  \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*

RUN mkdir /sqlancer && \
	wget -q -O- https://github.com/sqlancer/sqlancer/archive/main.tar.gz | \
	tar zx -C /sqlancer && \
    find /sqlancer -type d -exec chmod 777 {} \; && \
    find /sqlancer -type f -exec chmod 666 {} \; && \
	cd /sqlancer/sqlancer-main && \
	mvn --no-transfer-progress package -DskipTests && \
	rm -r /root/.m2
