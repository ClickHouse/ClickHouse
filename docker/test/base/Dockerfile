# rebuild in #33610
# docker build -t clickhouse/test-base .
ARG FROM_TAG=latest
FROM clickhouse/test-util:$FROM_TAG

RUN apt-get update \
    && apt-get install \
        lcov \
        netbase \
        perl \
        pv \
        ripgrep \
        zstd \
        locales \
        sudo \
        --yes --no-install-recommends \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*

# Install ODBC Bridge and Library Bridge, which are separately built components.
RUN BRIDGE_VERSION='25.1.5.31' \
    && ARCH=$(dpkg --print-architecture) \
    && cd /tmp \
    && wget "https://packages.clickhouse.com/deb/pool/main/c/clickhouse/clickhouse-odbc-bridge_${BRIDGE_VERSION}_${ARCH}.deb" \
    && wget "https://packages.clickhouse.com/deb/pool/main/c/clickhouse/clickhouse-library-bridge_${BRIDGE_VERSION}_${ARCH}.deb" \
    && dpkg -i clickhouse-odbc-bridge_${BRIDGE_VERSION}_${ARCH}.deb clickhouse-library-bridge_${BRIDGE_VERSION}_${ARCH}.deb \
    && rm clickhouse-odbc-bridge_${BRIDGE_VERSION}_${ARCH}.deb clickhouse-library-bridge_${BRIDGE_VERSION}_${ARCH}.deb

RUN echo "en_US.UTF-8 UTF-8" > /etc/locale.gen && locale-gen en_US.UTF-8
ENV LC_ALL en_US.UTF-8

ENV TZ=Europe/Amsterdam
RUN ln -snf "/usr/share/zoneinfo/$TZ" /etc/localtime && echo "$TZ" > /etc/timezone

# This script is used to setup realtime export of server logs from the CI into external ClickHouse cluster:
COPY setup_export_logs.sh /
COPY basic_helpers.sh /

CMD sleep 1
