# docker build -t clickhouse/sqlancer-test .
FROM ubuntu:22.04

# ARG for quick switch to a given ubuntu mirror
ARG apt_archive="http://archive.ubuntu.com"
RUN sed -i "s|http://archive.ubuntu.com|$apt_archive|g" /etc/apt/sources.list

RUN apt-get update --yes \
    && env DEBIAN_FRONTEND=noninteractive apt-get install wget git python3 default-jdk maven --yes --no-install-recommends  \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*

# We need to get the repository's HEAD each time despite, so we invalidate layers' cache
ARG CACHE_INVALIDATOR=0
RUN mkdir /sqlancer && \
	wget -q -O- https://github.com/sqlancer/sqlancer/archive/main.tar.gz | \
		tar zx -C /sqlancer && \
	cd /sqlancer/sqlancer-main && \
	mvn --no-transfer-progress package -DskipTests && \
	rm -r /root/.m2

COPY run.sh /
COPY process_sqlancer_result.py /
CMD ["/bin/bash", "/run.sh"]
