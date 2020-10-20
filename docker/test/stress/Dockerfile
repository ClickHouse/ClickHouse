# docker build -t yandex/clickhouse-stress-test .
FROM yandex/clickhouse-stateful-test

RUN apt-get update -y \
    && env DEBIAN_FRONTEND=noninteractive \
        apt-get install --yes --no-install-recommends \
            bash \
            tzdata \
            fakeroot \
            debhelper \
            parallel \
            expect \
            python3 \
            python3-lxml \
            python3-termcolor \
            python3-requests \
            curl \
            sudo \
            openssl \
            netcat-openbsd \
            telnet \
            llvm-9 \
            brotli

COPY ./stress /stress
COPY run.sh /

ENV DATASETS="hits visits"
CMD ["/bin/bash", "/run.sh"]
