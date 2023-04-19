# rebuild in #33610
# docker build -t clickhouse/stress-test .
ARG FROM_TAG=latest
FROM clickhouse/stateful-test:$FROM_TAG

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
ENV S3_URL="https://clickhouse-datasets.s3.amazonaws.com"
ENV EXPORT_S3_STORAGE_POLICIES=1

CMD ["/bin/bash", "/run.sh"]
