# docker build -t yandex/clickhouse-style-test .
FROM ubuntu:20.04

RUN sed -i 's|http://archive|http://ru.archive|g' /etc/apt/sources.list

RUN apt-get update && env DEBIAN_FRONTEND=noninteractive apt-get install --yes \
    shellcheck \
    libxml2-utils \
    git \
    python3-pip \
    pylint \
    yamllint \
    && pip3 install codespell


# For |& syntax
SHELL ["bash", "-c"]

CMD cd /ClickHouse/utils/check-style && \
    ./check-style -n              |& tee /test_output/style_output.txt && \
    ./check-typos                 |& tee /test_output/typos_output.txt && \
    ./check-whitespaces -n        |& tee /test_output/whitespaces_output.txt && \
    ./check-duplicate-includes.sh |& tee /test_output/duplicate_output.txt && \
    ./shellcheck-run.sh           |& tee /test_output/shellcheck_output.txt && \
    true
