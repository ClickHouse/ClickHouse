FROM ubuntu:18.04

ARG repository="deb http://repo.yandex.ru/clickhouse/deb/stable/ main/"
ARG version=18.13.0
ARG gosu_ver=1.10

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
        apt-transport-https \
        dirmngr \
        gnupg \
    && mkdir -p /etc/apt/sources.list.d \
    && apt-key adv --keyserver keyserver.ubuntu.com --recv E0C56BD4 \
    && echo $repository > /etc/apt/sources.list.d/clickhouse.list \
    && apt-get update \
    && env DEBIAN_FRONTEND=noninteractive \
        apt-get install --allow-unauthenticated --yes --no-install-recommends \
            clickhouse-common-static=$version \
            clickhouse-server=$version \
            libgcc-7-dev \
    && rm -rf \
        /var/lib/apt/lists/* \
        /var/cache/debconf \
        /tmp/* \
    && apt-get clean

COPY docker_related_config.xml /etc/clickhouse-server/config.d/
COPY entrypoint.sh /entrypoint.sh
ADD https://github.com/tianon/gosu/releases/download/1.10/gosu-amd64 /bin/gosu

RUN chmod +x \
    /entrypoint.sh \
    /bin/gosu

EXPOSE 9000 8123 9009
VOLUME /var/lib/clickhouse

ENV CLICKHOUSE_CONFIG /etc/clickhouse-server/config.xml

ENTRYPOINT ["/entrypoint.sh"]

CMD ["/usr/bin/clickhouse-server", "--config=${CLICKHOUSE_CONFIG}"]
