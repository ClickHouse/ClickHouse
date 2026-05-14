# docker build -t clickhouse/sqlancer-pp-test .
#
# SQLancer++ (https://github.com/suyZhong/SQLancerPlusPlus) is a generalised
# successor to upstream SQLancer that ships a built-in ClickHouse engine in its
# `general` provider. The image bakes a pre-built `sqlancer-2.0.0.jar` plus its
# dependency JARs so the job script only has to launch `java -jar ...`.
FROM ubuntu:22.04

ARG apt_archive="http://archive.ubuntu.com"
RUN sed -i -e "s|http://archive.ubuntu.com|$apt_archive|g" -e "s|https://archive.ubuntu.com|$apt_archive|g" /etc/apt/sources.list

RUN apt-get update \
    && apt-get install ca-certificates --yes --no-install-recommends --verbose-versions

RUN apt-get update --yes \
    && env DEBIAN_FRONTEND=noninteractive apt-get install \
        curl git python3 openjdk-17-jdk-headless maven \
        --yes --no-install-recommends \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*

# Pin to a known-good revision of SQLancer++ so the image is reproducible.
# Bump deliberately when a newer upstream is needed.
ARG SQLANCER_PP_REF=9442649e63d55a86f556da5aef832fac82280a2e

COPY swap-clickhouse-jdbc.py /tmp/swap-clickhouse-jdbc.py

# The swap script (see swap-clickhouse-jdbc.py for details):
#   * replaces upstream's legacy `ru.yandex.clickhouse:clickhouse-jdbc:0.3.2`
#     (response-stream parser is incompatible with ClickHouse >= 25.x) with
#     the latest `com.clickhouse:clickhouse-jdbc` shaded-all artifact, version
#     resolved from Maven Central at build time;
#   * extends `dbconfigs/jdbc.properties` to embed user/password in the JDBC
#     URL template, which the modern driver requires;
#   * rewrites `GeneralNoRECOracle.java`'s `IS TRUE` postfix to `!= 0`,
#     because ClickHouse's parser doesn't implement the SQL-standard
#     `IS TRUE`/`IS FALSE` operators and a strict `= TRUE` would produce
#     false-positive oracle mismatches for `WHERE LN(x)`-style truthy checks
#     where the value is non-zero but not equal to 1.
# No SQLancer++ source code references either driver package, so the runtime
# swap is safe.
RUN mkdir /sqlancer-pp \
    && curl --fail --silent --show-error --location \
       --retry 5 --retry-delay 5 --retry-all-errors \
       "https://github.com/suyZhong/SQLancerPlusPlus/archive/${SQLANCER_PP_REF}.tar.gz" \
       -o /tmp/sqlancer-pp.tar.gz \
    && tar zxf /tmp/sqlancer-pp.tar.gz -C /sqlancer-pp --strip-components=1 \
    && rm /tmp/sqlancer-pp.tar.gz \
    && python3 /tmp/swap-clickhouse-jdbc.py /sqlancer-pp \
    && rm /tmp/swap-clickhouse-jdbc.py \
    && cd /sqlancer-pp \
    && mvn --no-transfer-progress package -DskipTests \
    && find /sqlancer-pp -type d -exec chmod 777 {} \; \
    && find /sqlancer-pp -type f -exec chmod 666 {} \; \
    && rm -r /root/.m2
