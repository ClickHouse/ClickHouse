# docker build -t clickhouse/postgresql-java-client .
# PostgreSQL Java client docker container
# Rebuild to enable SBOM/provenance attestations (see #97903, #98511)

FROM ubuntu:24.04

# Redirect apt to the mirror passed by CI (canonical default; see ci/praktika/docker.py).
# Rewrite archive.ubuntu.com (amd64) and ports.ubuntu.com (arm64) across sources.list
# and deb822 *.sources so arm64 builds avoid the flaky ports.ubuntu.com.
ARG apt_archive="http://archive.ubuntu.com"
ARG apt_ports_archive="http://ports.ubuntu.com"
RUN find /etc/apt -type f \( -name '*.list' -o -name '*.sources' \) -print0 | xargs -0 -r sed -i \
        -e "s|http://archive.ubuntu.com|$apt_archive|g" \
        -e "s|https://archive.ubuntu.com|$apt_archive|g" \
        -e "s|http://ports.ubuntu.com|$apt_ports_archive|g" \
        -e "s|https://ports.ubuntu.com|$apt_ports_archive|g"

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
