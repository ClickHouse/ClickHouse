FROM ubuntu:14.04

RUN apt-get update && \
    apt-get install -y apt-transport-https && \
    mkdir -p /etc/apt/sources.list.d && \
    echo "deb https://repo.yandex.ru/clickhouse/trusty/ dists/stable/main/binary-amd64/" | \
         tee /etc/apt/sources.list.d/clickhouse.list && \
    apt-get update && \
    apt-get install --allow-unauthenticated -y clickhouse-server-common && \
    rm -rf /var/lib/apt/lists/* /var/cache/

RUN chown -R metrika /etc/clickhouse-server/

USER metrika
EXPOSE 9000 8123 9009

ENV CLICKHOUSE_CONFIG /etc/clickhouse-server/config.xml

ENTRYPOINT exec /usr/bin/clickhouse-server --config=${CLICKHOUSE_CONFIG}
