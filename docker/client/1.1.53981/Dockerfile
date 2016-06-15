FROM ubuntu:14.04

ENV CLICKHOUSE_VERSION 1.1.53981

RUN mkdir -p /etc/apt/sources.list.d && \
    echo "deb http://repo.yandex.ru/clickhouse/trusty/ dists/stable/main/binary-amd64/" | tee /etc/apt/sources.list.d/clickhouse.list && \
    apt-get update && \
    apt-get install --force-yes -y clickhouse-client="$CLICKHOUSE_VERSION" && \
    rm -rf /var/lib/apt/lists/* /var/cache/

ENV CLICKHOUSE_HOST localhost
ENV CLICKHOUSE_PORT 9000

CMD ["sh", "-c", "clickhouse-client --host ${CLICKHOUSE_HOST} --port ${CLICKHOUSE_PORT}"]
