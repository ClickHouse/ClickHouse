# rebuild in #33610
# docker build -t clickhouse/stateful-test .
ARG FROM_TAG=latest
FROM clickhouse/stateless-test:$FROM_TAG

RUN apt-get update -y \
    && env DEBIAN_FRONTEND=noninteractive \
        apt-get install --yes --no-install-recommends \
        python3-requests \
    && apt-get clean

COPY s3downloader /s3downloader

ENV S3_URL="https://clickhouse-datasets.s3.amazonaws.com"
ENV DATASETS="hits visits"

COPY run.sh /
CMD ["/bin/bash", "/run.sh"]
