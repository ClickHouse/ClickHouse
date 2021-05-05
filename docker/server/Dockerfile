FROM ubuntu:20.04

ARG repository="deb https://repo.clickhouse.tech/deb/stable/ main/"
ARG version=21.6.1.*
ARG gosu_ver=1.10

# set non-empty deb_location_url url to create a docker image
# from debs created by CI build, for example:
# docker build . --network host --build-arg version="21.4.1.6282" --build-arg deb_location_url="https://clickhouse-builds.s3.yandex.net/21852/069cfbff388b3d478d1a16dc7060b48073f5d522/clickhouse_build_check/clang-11_relwithdebuginfo_none_bundled_unsplitted_disable_False_deb/" -t filimonovq/clickhouse-server:pr21852
ARG deb_location_url=""

# set non-empty single_binary_location_url to create docker image
# from a single binary url (useful for non-standard builds - with sanitizers, for arm64).
# for example (run on aarch64 server):
# docker build . --network host --build-arg single_binary_location_url="https://builds.clickhouse.tech/master/aarch64/clickhouse" -t altinity/clickhouse-server:master-testing-arm
# note: clickhouse-odbc-bridge is not supported there.
ARG single_binary_location_url=""

# see https://github.com/moby/moby/issues/4032#issuecomment-192327844
ARG DEBIAN_FRONTEND=noninteractive

# user/group precreated explicitly with fixed uid/gid on purpose.
# It is especially important for rootless containers: in that case entrypoint
# can't do chown and owners of mounted volumes should be configured externally.
# We do that in advance at the begining of Dockerfile before any packages will be
# installed to prevent picking those uid / gid by some unrelated software.
# The same uid / gid (101) is used both for alpine and ubuntu.

RUN groupadd -r clickhouse --gid=101 \
    && useradd -r -g clickhouse --uid=101 --home-dir=/var/lib/clickhouse --shell=/bin/bash clickhouse \
    && apt-get update \
    && apt-get install --yes --no-install-recommends \
        apt-transport-https \
        ca-certificates \
        dirmngr \
        gnupg \
        locales \
        wget \
        tzdata \
    && mkdir -p /etc/apt/sources.list.d \
    && apt-key adv --keyserver keyserver.ubuntu.com --recv E0C56BD4 \
    && echo $repository > /etc/apt/sources.list.d/clickhouse.list \
    && if [ -n "$deb_location_url" ]; then \
            echo "installing from custom url with deb packages: $deb_location_url" \
            rm -rf /tmp/clickhouse_debs \
            && mkdir -p /tmp/clickhouse_debs \
            && wget --progress=bar:force:noscroll "${deb_location_url}/clickhouse-common-static_${version}_amd64.deb" -P /tmp/clickhouse_debs \
            && wget --progress=bar:force:noscroll "${deb_location_url}/clickhouse-client_${version}_all.deb" -P /tmp/clickhouse_debs \
            && wget --progress=bar:force:noscroll "${deb_location_url}/clickhouse-server_${version}_all.deb" -P /tmp/clickhouse_debs \
            && dpkg -i /tmp/clickhouse_debs/*.deb ; \
       elif [ -n "$single_binary_location_url" ]; then \
            echo "installing from single binary url: $single_binary_location_url" \
            && rm -rf /tmp/clickhouse_binary \
            && mkdir -p /tmp/clickhouse_binary \
            && wget --progress=bar:force:noscroll "$single_binary_location_url" -O /tmp/clickhouse_binary/clickhouse \
            && chmod +x /tmp/clickhouse_binary/clickhouse \
            && /tmp/clickhouse_binary/clickhouse install --user "clickhouse" --group "clickhouse" ; \
       else \
           echo "installing from repository: $repository" \
           && apt-get update \
           && apt-get --yes -o "Dpkg::Options::=--force-confdef" -o "Dpkg::Options::=--force-confold" upgrade \
           && apt-get install --allow-unauthenticated --yes --no-install-recommends \
                clickhouse-common-static=$version \
                clickhouse-client=$version \
                clickhouse-server=$version ; \
       fi \
    && wget --progress=bar:force:noscroll "https://github.com/tianon/gosu/releases/download/$gosu_ver/gosu-$(dpkg --print-architecture)" -O /bin/gosu \
    && chmod +x /bin/gosu \
    && clickhouse-local -q 'SELECT * FROM system.build_options' \
    && rm -rf \
        /var/lib/apt/lists/* \
        /var/cache/debconf \
        /tmp/* \
    && apt-get clean \
    && mkdir -p /var/lib/clickhouse /var/log/clickhouse-server /etc/clickhouse-server /etc/clickhouse-client \
    && chmod ugo+Xrw -R /var/lib/clickhouse /var/log/clickhouse-server /etc/clickhouse-server /etc/clickhouse-client

# we need to allow "others" access to clickhouse folder, because docker container
# can be started with arbitrary uid (openshift usecase)

RUN locale-gen en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8
ENV TZ UTC

RUN mkdir /docker-entrypoint-initdb.d

COPY docker_related_config.xml /etc/clickhouse-server/config.d/
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 9000 8123 9009
VOLUME /var/lib/clickhouse

ENV CLICKHOUSE_CONFIG /etc/clickhouse-server/config.xml

ENTRYPOINT ["/entrypoint.sh"]

