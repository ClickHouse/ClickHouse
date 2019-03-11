FROM ubuntu:18.04

ARG gosu_ver=1.10
ARG CLICKHOUSE_PACKAGES_DIR

COPY ${CLICKHOUSE_PACKAGES_DIR}/clickhouse-*.deb /packages/

# installing via apt to simulate real-world scenario, where user installs deb package and all it's dependecies automatically.
RUN apt update; \
    DEBIAN_FRONTEND=noninteractive \
    apt install -y \
        /packages/clickhouse-common-static_*.deb \
        /packages/clickhouse-server_*.deb \
        locales ;\
    rm -rf /packages

ADD https://github.com/tianon/gosu/releases/download/${gosu_ver}/gosu-amd64 /bin/gosu

RUN locale-gen en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8

RUN mkdir /docker-entrypoint-initdb.d

COPY server/docker_related_config.xml /etc/clickhouse-server/config.d/
COPY server/entrypoint.sh /entrypoint.sh

RUN chmod +x \
    /entrypoint.sh \
    /bin/gosu

EXPOSE 9000 8123 9009
VOLUME /var/lib/clickhouse

ENV CLICKHOUSE_CONFIG /etc/clickhouse-server/config.xml

ENTRYPOINT ["/entrypoint.sh"]
