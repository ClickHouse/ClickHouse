FROM ubuntu:16.04

ARG repository="deb http://repo.yandex.ru/clickhouse/deb/stable/ main/"
ARG version=\*

RUN apt-get update && \
    apt-get install -y apt-transport-https gosu && \
    mkdir -p /etc/apt/sources.list.d && \
    echo $repository | tee /etc/apt/sources.list.d/clickhouse.list && \
    apt-get update && \
    apt-get install --allow-unauthenticated -y clickhouse-server-common=$version clickhouse-server-base=$version && \
    rm -rf /var/lib/apt/lists/* /var/cache/debconf && \
    apt-get clean

COPY docker_related_config.xml /etc/clickhouse-server/config.d/
RUN chown -R clickhouse /etc/clickhouse-server/

EXPOSE 9000 8123 9009
VOLUME /var/lib/clickhouse

COPY entrypoint.sh /usr/local/bin/entrypoint.sh

ENV \
  CLICKHOUSE_CONFIG=/etc/clickhouse-server/config.xml \
  \
  USER_UID=105 \
  USER_GID=106

ENTRYPOINT /usr/local/bin/entrypoint.sh
