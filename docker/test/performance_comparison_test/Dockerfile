FROM yandex/clickhouse-stateful-test

RUN apt-get update -y \
    && env DEBIAN_FRONTEND=noninteractive \
        apt-get install --yes --no-install-recommends \
            python-requests

COPY run.sh /run.sh
CMD /run.sh