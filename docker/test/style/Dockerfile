# docker build -t clickhouse/style-test .
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

COPY run.sh /
COPY process_style_check_result.py /
CMD ["/bin/bash", "/run.sh"]
