# docker build -t clickhouse/libfuzzer .
ARG FROM_TAG=latest
FROM clickhouse/test-base:$FROM_TAG

# ARG for quick switch to a given ubuntu mirror
ARG apt_archive="http://archive.ubuntu.com"
RUN sed -i "s|http://archive.ubuntu.com|$apt_archive|g" /etc/apt/sources.list

ENV LANG=C.UTF-8
ENV TZ=Europe/Amsterdam
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install --yes --no-install-recommends \
            ca-certificates \
            libc6-dbg \
            moreutils \
            ncdu \
            p7zip-full \
            parallel \
            psmisc \
            python3 \
            python3-pip \
            rsync \
            tree \
            tzdata \
            vim \
            wget \
    && apt-get autoremove --yes \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*

COPY requirements.txt /
RUN pip3 install --no-cache-dir -r /requirements.txt

SHELL ["/bin/bash", "-c"]

# docker run --network=host --volume <workspace>:/workspace -e PR_TO_TEST=<> -e SHA_TO_TEST=<> clickhouse/libfuzzer

