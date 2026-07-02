# docker build -t clickhouse/sqlancer-test .
#
# This image hosts two fuzzers that both run against clickhouse-server over the
# `com.clickhouse` client-v2 transport under JDK 25:
#   * SQLancer (ClickHouse/sqlancer) in /sqlancer/sqlancer-main
#   * SQLancer++ (suyZhong/SQLancerPlusPlus) in /sqlancer-pp
FROM ubuntu:22.04

# ARG for quick switch to a given ubuntu mirror
ARG apt_archive="http://archive.ubuntu.com"
RUN sed -i -e "s|http://archive.ubuntu.com|$apt_archive|g" -e "s|https://archive.ubuntu.com|$apt_archive|g" /etc/apt/sources.list

# We need ca-certificates first to be able to update all repos. This is why it's run twice
RUN apt-get update \
    && apt-get install ca-certificates --yes --no-install-recommends --verbose-versions

# The SQLancer ClickHouse provider (ClickHouse/sqlancer) now targets Java 25
# (`<release>25</release>` in `pom.xml`) and the `com.clickhouse:client-v2`
# transport, so the Ubuntu `default-jdk` (11/17) and `maven` (3.6.3) can no
# longer build it. Install Temurin JDK 25 from Adoptium and a recent Maven.
RUN apt-get update --yes \
    && env DEBIAN_FRONTEND=noninteractive apt-get install wget git python3 gnupg apt-transport-https --yes --no-install-recommends \
    && mkdir -p /etc/apt/keyrings \
    && wget -qO- https://packages.adoptium.net/artifactory/api/gpg/key/public | gpg --dearmor -o /etc/apt/keyrings/adoptium.gpg \
    && echo "deb [signed-by=/etc/apt/keyrings/adoptium.gpg] https://packages.adoptium.net/artifactory/deb jammy main" > /etc/apt/sources.list.d/adoptium.list \
    && apt-get update --yes \
    && env DEBIAN_FRONTEND=noninteractive apt-get install temurin-25-jdk --yes --no-install-recommends \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/* \
    # The Temurin deb installs to an arch-specific dir (temurin-25-jdk-amd64 /
    # -arm64); this image is built for both arches, so symlink a stable path.
    && ln -s /usr/lib/jvm/temurin-25-jdk-* /usr/lib/jvm/temurin-25-jdk

ENV JAVA_HOME="/usr/lib/jvm/temurin-25-jdk"

# Vendored Maven 3.9.9 (matches the upstream sqlancer build), new enough to run
# on JDK 25; the Ubuntu 3.6.3 package is too old for the build plugins.
RUN wget -q https://archive.apache.org/dist/maven/maven-3/3.9.9/binaries/apache-maven-3.9.9-bin.tar.gz -O /tmp/maven.tar.gz \
    && tar -xzf /tmp/maven.tar.gz -C /opt \
    && rm /tmp/maven.tar.gz
ENV PATH="/opt/apache-maven-3.9.9/bin:${PATH}"

# ClickHouse/sqlancer @ main after PR #4 (wrong-result oracle suite, 31 oracles,
# recursive type system, client-v2/RowBinary transport).
# https://github.com/ClickHouse/sqlancer/pull/4
RUN mkdir /sqlancer && \
	wget -q -O- https://github.com/ClickHouse/sqlancer/archive/a62ea509ee53862b5dd37f452e8c2edce8038951.tar.gz | \
	tar zx -C /sqlancer && \
	mv /sqlancer/sqlancer-a62ea509ee53862b5dd37f452e8c2edce8038951 /sqlancer/sqlancer-main && \
    find /sqlancer -type d -exec chmod 777 {} \; && \
    find /sqlancer -type f -exec chmod 666 {} \; && \
	cd /sqlancer/sqlancer-main && \
	mvn --no-transfer-progress package -Dmaven.test.skip=true -Djacoco.skip=true && \
	rm -r /root/.m2

# SQLancer++ (https://github.com/suyZhong/SQLancerPlusPlus): a generalised
# successor to upstream SQLancer with a built-in ClickHouse engine in its
# `general` provider. Built into this same image so the SQLancer++ CI job can
# reuse it (JDK 25 + the client-v2 clickhouse-jdbc driver) instead of a separate
# image. `swap-clickhouse-jdbc.py` pins the JDBC driver to the client-v2 release
# and applies the ClickHouse-specific oracle/config patches (see the script).
# `-Djacoco.skip=true`: jacoco 0.8.8 (pinned by SQLancer++) can't read JDK 25
# class files; tests are skipped so the coverage agent is not needed anyway.
ARG SQLANCER_PP_REF=9442649e63d55a86f556da5aef832fac82280a2e
COPY swap-clickhouse-jdbc.py /tmp/swap-clickhouse-jdbc.py
RUN mkdir /sqlancer-pp && \
	wget -q -O- https://github.com/suyZhong/SQLancerPlusPlus/archive/${SQLANCER_PP_REF}.tar.gz | \
	tar zx -C /sqlancer-pp --strip-components=1 && \
	python3 /tmp/swap-clickhouse-jdbc.py /sqlancer-pp && \
	rm /tmp/swap-clickhouse-jdbc.py && \
    find /sqlancer-pp -type d -exec chmod 777 {} \; && \
    find /sqlancer-pp -type f -exec chmod 666 {} \; && \
	cd /sqlancer-pp && \
	mvn --no-transfer-progress package -DskipTests -Djacoco.skip=true && \
	rm -r /root/.m2
