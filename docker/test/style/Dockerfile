# docker build -t yandex/clickhouse-style-test .
FROM ubuntu:20.04

RUN apt-get update && env DEBIAN_FRONTEND=noninteractive apt-get install --yes shellcheck libxml2-utils git python3-pip && pip3 install codespell


CMD cd /ClickHouse/utils/check-style && ./check-style -n | tee /test_output/style_output.txt && \
    ./check-duplicate-includes.sh | tee /test_output/duplicate_output.txt
