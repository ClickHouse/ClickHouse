# docker build -t yandex/clickhouse-performance-comparison .
FROM ubuntu:18.04

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
        p7zip-full bash ncdu wget python3 python3-pip python3-dev g++ \
    && pip3 --no-cache-dir install clickhouse_driver \
    && apt-get purge --yes python3-dev g++ \
    && apt-get autoremove --yes \
    && apt-get clean

COPY * /

CMD /entrypoint.sh

# docker run --network=host --volume <workspace>:/workspace --volume=<output>:/output -e LEFT_PR=<> -e LEFT_SHA=<> -e RIGHT_PR=<> -e RIGHT_SHA=<> yandex/clickhouse-performance-comparison

