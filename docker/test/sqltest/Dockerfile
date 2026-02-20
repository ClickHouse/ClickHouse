# docker build -t clickhouse/sqltest .
ARG FROM_TAG=latest
FROM clickhouse/test-base:$FROM_TAG

RUN apt-get update --yes \
    && env DEBIAN_FRONTEND=noninteractive \
        apt-get install --yes --no-install-recommends \
            wget \
            git \
            python3 \
            python3-dev \
            python3-pip \
            sudo \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*

COPY requirements.txt /
RUN pip3 install --no-cache-dir -r /requirements.txt

ARG sqltest_repo="https://github.com/elliotchance/sqltest/"

RUN git clone ${sqltest_repo}

ENV TZ=UTC
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

COPY run.sh /
COPY test.py /
CMD ["/bin/bash", "/run.sh"]
