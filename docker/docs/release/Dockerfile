# docker build -t clickhouse/docs-release .
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
            fonts-arphic-ukai \
            fonts-arphic-uming \
            fonts-ipafont-mincho \
            fonts-ipafont-gothic \
            fonts-unfonts-core \
            xvfb \
            ssh-client \
    && apt-get autoremove --yes \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install --ignore-installed --upgrade setuptools pip virtualenv

# We create the most popular default 1000:1000 ubuntu user to not have ssh issues when running with UID==1000
RUN useradd --create-home --uid 1000 --user-group ubuntu \
  && ssh-keyscan -t rsa github.com >> /etc/ssh/ssh_known_hosts

COPY run.sh /

ENV REPO_PATH=/repo_path
ENV OUTPUT_PATH=/output_path

CMD ["/bin/bash", "/run.sh"]
