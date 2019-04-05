# Since right now we can't set volumes to the docker during build, we split building container in stages:
# 1. build base container
# 2. run base conatiner with mounted volumes
# 3. commit container as image
# 4. build final container atop that image
# Middle steps are performed by the bash script.

FROM ubuntu:18.04 as clickhouse-server-base
ARG gosu_ver=1.10

VOLUME /packages/

# update to allow installing dependencies of clickhouse automatically
RUN apt update; \
    DEBIAN_FRONTEND=noninteractive \
    apt install -y locales;

ADD https://github.com/tianon/gosu/releases/download/${gosu_ver}/gosu-amd64 /bin/gosu

RUN locale-gen en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8

# installing via apt to simulate real-world scenario, where user installs deb package and all it's dependecies automatically.
CMD DEBIAN_FRONTEND=noninteractive \
    apt install -y \
        /packages/clickhouse-common-static_*.deb \
        /packages/clickhouse-server_*.deb ;

FROM clickhouse-server-base:postinstall as clickhouse-server

RUN mkdir /docker-entrypoint-initdb.d

COPY docker_related_config.xml /etc/clickhouse-server/config.d/
COPY entrypoint.sh /entrypoint.sh

RUN chmod +x \
    /entrypoint.sh \
    /bin/gosu

EXPOSE 9000 8123 9009
VOLUME /var/lib/clickhouse

ENV CLICKHOUSE_CONFIG /etc/clickhouse-server/config.xml

ENTRYPOINT ["/entrypoint.sh"]
