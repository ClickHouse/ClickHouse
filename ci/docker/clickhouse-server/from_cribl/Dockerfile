# Use the internal Cribl base image
ARG IMAGE_REPO
FROM ${IMAGE_REPO}:22.04

# see https://github.com/moby/moby/issues/4032#issuecomment-192327844
# It could be removed after we move on a version 23:04+
ARG DEBIAN_FRONTEND=noninteractive

# ARG for quick switch to a given ubuntu mirror
ARG apt_archive="http://archive.ubuntu.com"

# We shouldn't use `apt upgrade` to not change the upstream image. It's updated biweekly

# user/group precreated explicitly with fixed uid/gid on purpose.
# It is especially important for rootless containers: in that case entrypoint
# can't do chown and owners of mounted volumes should be configured externally.
# We do that in advance at the begining of Dockerfile before any packages will be
# installed to prevent picking those uid / gid by some unrelated software.
# The same uid / gid (101) is used both for alpine and ubuntu.
RUN sed -i "s|http://archive.ubuntu.com|${apt_archive}|g" /etc/apt/sources.list \
    && groupadd -r clickhouse --gid=101 \
    && useradd -r -g clickhouse --uid=101 --home-dir=/var/lib/clickhouse --shell=/bin/bash clickhouse \
    && apt-get update \
    && apt-get install --yes --no-install-recommends \
        ca-certificates \
        locales \
        tzdata \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*

COPY ./clickhouse /tmp/clickhouse
RUN ls -l /tmp/clickhouse
RUN chmod +x /tmp/clickhouse \
    && /tmp/clickhouse install --user "clickhouse" --group "clickhouse" \
    && rm -rf /tmp/*

# post install
# we need to allow "others" access to clickhouse folder, because docker container
# can be started with arbitrary uid (openshift usecase)
RUN clickhouse-local -q 'SELECT * FROM system.build_options' \
    && mkdir -p /var/lib/clickhouse /var/log/clickhouse-server /etc/clickhouse-server /etc/clickhouse-client \
    && chmod ugo+Xrw -R /var/lib/clickhouse /var/log/clickhouse-server /etc/clickhouse-server /etc/clickhouse-client

RUN locale-gen en_US.UTF-8
ENV LANG en_US.UTF-8
ENV TZ UTC

RUN mkdir /docker-entrypoint-initdb.d

COPY docker_related_config.xml /etc/clickhouse-server/config.d/
COPY entrypoint.sh /entrypoint.sh

EXPOSE 9000 8123 9009
VOLUME /var/lib/clickhouse

ENV CLICKHOUSE_CONFIG /etc/clickhouse-server/config.xml

ENTRYPOINT ["/entrypoint.sh"]
