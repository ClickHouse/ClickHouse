FROM ubuntu:16.04

ARG repository="deb https://repo.yandex.ru/clickhouse/xenial/ dists/stable/main/binary-amd64/"
ARG version=\*

RUN apt update && \
    apt install -y apt-transport-https && \
    mkdir -p /etc/apt/sources.list.d && \
    echo $repository | tee /etc/apt/sources.list.d/clickhouse.list && \
    apt update && \
    apt install --allow-unauthenticated -y clickhouse-server-common=$version clickhouse-server-base=$version && \
    rm -rf /var/lib/apt/lists/* /var/cache/debconf && \
    apt clean

COPY docker_related_config.xml /etc/clickhouse-server/config.d/
RUN chown -R clickhouse /etc/clickhouse-server/

USER clickhouse
EXPOSE 9000 8123 9009
VOLUME /var/lib/clickhouse

ENV CLICKHOUSE_CONFIG /etc/clickhouse-server/config.xml

ENTRYPOINT exec /usr/bin/clickhouse-server --config=${CLICKHOUSE_CONFIG}
