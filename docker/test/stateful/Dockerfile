# rebuild in #47031
# docker build -t clickhouse/stateful-test .
ARG FROM_TAG=latest
FROM clickhouse/stateless-test:$FROM_TAG

RUN apt-get update -y \
    && env DEBIAN_FRONTEND=noninteractive \
        apt-get install --yes --no-install-recommends \
        python3-requests \
        nodejs \
        npm \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*

COPY create.sql /
COPY run.sh /
CMD ["/bin/bash", "/run.sh"]
