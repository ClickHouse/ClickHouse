FROM ubuntu:14.04

RUN apt-get update && \
    apt-get install -y apt-transport-https && \
    mkdir -p /etc/apt/sources.list.d && \
    echo "deb https://repo.yandex.ru/clickhouse/trusty/ dists/stable/main/binary-amd64/" | \
         tee /etc/apt/sources.list.d/clickhouse.list && \
    apt-get update && \
    apt-get install --allow-unauthenticated -y clickhouse-client && \
    rm -rf /var/lib/apt/lists/* /var/cache/

ENTRYPOINT ["/usr/bin/clickhouse-client"]
