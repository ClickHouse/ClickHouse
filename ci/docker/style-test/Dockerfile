# docker build -t clickhouse/style-test .
FROM ubuntu:22.04

RUN apt-get update && env DEBIAN_FRONTEND=noninteractive apt-get install --yes \
        aspell \
        libxml2-utils \
        python3-pip \
        locales \
        ripgrep \
        curl \
        git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*

ARG ASPELL_VERSION=2020.12.07-0
RUN curl -O https://ftp.gnu.org/gnu/aspell/dict/en/aspell6-en-$ASPELL_VERSION.tar.bz2 && \
    curl -O https://ftp.gnu.org/gnu/aspell/dict/en/aspell6-en-$ASPELL_VERSION.tar.bz2.sig && \
    gpg --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 71C636695B147849 && \
    gpg --verify aspell6-en-$ASPELL_VERSION.tar.bz2.sig aspell6-en-$ASPELL_VERSION.tar.bz2 && \
    tar xvf aspell6-en-$ASPELL_VERSION.tar.bz2 && \
    cd aspell6-en-$ASPELL_VERSION && \
    pwd && \
    ./configure && make && make install && \
    cd .. && \
    rm -fr aspell6-en-$ASPELL_VERSION*

RUN echo "en_US.UTF-8 UTF-8" > /etc/locale.gen && locale-gen en_US.UTF-8
ENV LC_ALL=en_US.UTF-8

COPY requirements.txt /
RUN pip3 install --no-cache-dir -r requirements.txt
