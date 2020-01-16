# docker build -t yandex/clickhouse-performance-comparison .
FROM ubuntu:18.04

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install --yes --no-install-recommends \
        p7zip-full bash git ncdu wget psmisc python3 python3-pip tzdata python3-dev g++ \
    && pip3 --no-cache-dir install clickhouse_driver \
    && apt-get purge --yes python3-dev g++ \
    && apt-get autoremove --yes \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY * /

CMD /entrypoint.sh

# docker run --network=host --volume <workspace>:/workspace --volume=<output>:/output -e PR_TO_TEST=<> -e SHA_TO_TEST=<> yandex/clickhouse-performance-comparison

