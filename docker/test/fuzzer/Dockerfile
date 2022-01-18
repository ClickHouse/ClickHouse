# docker build -t clickhouse/fuzzer .
FROM clickhouse/test-base

ENV LANG=C.UTF-8
ENV TZ=Europe/Moscow
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN sed -i 's|http://archive|http://ru.archive|g' /etc/apt/sources.list

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install --yes --no-install-recommends \
            ca-certificates \
            libc6-dbg \
            moreutils \
            ncdu \
            p7zip-full \
            parallel \
            psmisc \
            rsync \
            tree \
            tzdata \
            vim \
            wget \
    && apt-get autoremove --yes \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY * /

SHELL ["/bin/bash", "-c"]
CMD set -o pipefail \
    && cd /workspace \
    && /run-fuzzer.sh 2>&1 | ts "$(printf '%%Y-%%m-%%d %%H:%%M:%%S\t')" | tee main.log

# docker run --network=host --volume <workspace>:/workspace -e PR_TO_TEST=<> -e SHA_TO_TEST=<> clickhouse/fuzzer

