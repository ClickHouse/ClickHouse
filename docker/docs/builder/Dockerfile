# rebuild in #33610
# docker build -t clickhouse/docs-builder .
FROM ubuntu:20.04

# ARG for quick switch to a given ubuntu mirror
ARG apt_archive="http://archive.ubuntu.com"
RUN sed -i "s|http://archive.ubuntu.com|$apt_archive|g" /etc/apt/sources.list

ENV LANG=C.UTF-8

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install --yes --no-install-recommends \
            wget \
            bash \
            python \
            curl \
            python3-requests \
            sudo \
            git \
            openssl \
            python3-pip \
            software-properties-common \
            language-pack-zh* \
            chinese* \
            fonts-arphic-ukai \
            fonts-arphic-uming \
            fonts-ipafont-mincho \
            fonts-ipafont-gothic \
            fonts-unfonts-core \
            xvfb \
            nodejs \
            npm \
            openjdk-11-jdk \
            ssh-client \
    && pip --no-cache-dir install scipy \
    && apt-get autoremove --yes \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN wget 'https://github.com/wkhtmltopdf/packaging/releases/download/0.12.6-1/wkhtmltox_0.12.6-1.focal_amd64.deb'

RUN npm i -g purify-css

RUN pip3 install --ignore-installed --upgrade setuptools pip virtualenv
