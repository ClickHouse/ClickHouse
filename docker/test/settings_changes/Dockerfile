# rebuild in #58555
# docker build -t clickhouse/settings-changes-check .
ARG FROM_TAG=latest
FROM clickhouse/test-base:$FROM_TAG

RUN apt-get update -y \
    && env DEBIAN_FRONTEND=noninteractive \
        apt-get install --yes --no-install-recommends \
            bash \
            python3 \
            python3-lxml \
            python3-termcolor \
            python3-requests \
            curl \
            sudo \
    && apt-get clean

COPY run.sh /
CMD ["/bin/bash", "/run.sh"]
