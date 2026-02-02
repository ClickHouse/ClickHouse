ARG FROM_TAG=latest
FROM clickhouse/stateless-test:$FROM_TAG

USER root

RUN apt-get update -y \
    && env DEBIAN_FRONTEND=noninteractive \
        apt-get install --yes --no-install-recommends \
        nodejs \
        npm \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/* \

USER clickhouse
