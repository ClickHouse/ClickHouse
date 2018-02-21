FROM ubuntu:16.04

ARG repository="deb https://repo.yandex.ru/clickhouse/xenial/ dists/stable/main/binary-amd64/"
ARG version=\*

RUN apt update && \
    apt install -y apt-transport-https && \
    mkdir -p /etc/apt/sources.list.d && \
    echo $repository | tee /etc/apt/sources.list.d/clickhouse.list && \
    apt update && \
    apt install --allow-unauthenticated -y clickhouse-client=$version locales && \
    rm -rf /var/lib/apt/lists/* /var/cache/debconf && \
    apt clean

RUN locale-gen en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8

ENTRYPOINT ["/usr/bin/clickhouse-client"]
