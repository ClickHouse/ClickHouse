# docker build -t clickhouse/style-test .
FROM ubuntu:22.04

RUN apt-get update && env DEBIAN_FRONTEND=noninteractive apt-get install --yes \
        aspell \
        libxml2-utils \
        python3-pip \
        locales \
        git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*

RUN echo "en_US.UTF-8 UTF-8" > /etc/locale.gen && locale-gen en_US.UTF-8
ENV LC_ALL=en_US.UTF-8

COPY requirements.txt /
RUN pip3 install --no-cache-dir -r requirements.txt
