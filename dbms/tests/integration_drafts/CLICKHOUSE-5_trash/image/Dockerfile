FROM ubuntu:14.04

ARG repository="deb https://repo.yandex.ru/clickhouse/trusty/ dists/stable/main/binary-amd64/"
ARG version=\*

RUN apt-get update && \
    apt-get install -y apt-transport-https && \
    mkdir -p /etc/apt/sources.list.d && \
    echo $repository | tee /etc/apt/sources.list.d/clickhouse.list && \
    apt-get update && \
    apt-get install --allow-unauthenticated -y clickhouse-server-common=$version && \
    apt-get install --allow-unauthenticated -y clickhouse-client=$version && \
    rm -rf /var/lib/apt/lists/* /var/cache/debconf && \
    apt-get clean

RUN sed -i 's,<listen_host>127.0.0.1</listen_host>,<listen_host>::</listen_host>,' /etc/clickhouse-server/config.xml
RUN chown -R clickhouse /etc/clickhouse-server/

EXPOSE 9000 8123 9009
#VOLUME /var/lib/clickhouse

RUN mkdir -p /var/run/clickhouse-server && \
    chown -R clickhouse /var/run/clickhouse-server
#RUN mkdir -p /etc/clickhouse-server/conf.d/ /etc/clickhouse-server/users.d/ && \
#    chown -R clickhouse /etc/clickhouse-server/conf.d /etc/clickhouse-server/users.d/
RUN rm -rf /etc/clickhouse-server/conf.d /etc/clickhouse-server/users.d
RUN rm -f /usr/bin/clickhouse

VOLUME /etc/clickhouse-server/conf.d/
VOLUME /etc/clickhouse-server/users.d/
VOLUME /usr/bin/clickhouse

ENV LAYER 1
ENV SHARD 1
ENV REPLICA 1

COPY docker-entrypoint.sh /
ENTRYPOINT ["/docker-entrypoint.sh"]
