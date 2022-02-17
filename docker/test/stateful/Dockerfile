# rebuild in #33610
# docker build -t clickhouse/stateful-test .
ARG FROM_TAG=latest
FROM clickhouse/stateless-test:$FROM_TAG

RUN apt-get update -y \
    && env DEBIAN_FRONTEND=noninteractive \
        apt-get install --yes --no-install-recommends \
        python3-requests \
        llvm-9

COPY s3downloader /s3downloader

ENV S3_URL="https://clickhouse-datasets.s3.yandex.net"
ENV DATASETS="hits visits"

COPY run.sh /
CMD ["/bin/bash", "/run.sh"]
