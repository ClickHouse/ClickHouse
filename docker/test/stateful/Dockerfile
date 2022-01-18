# docker build -t clickhouse/stateful-test .
FROM clickhouse/stateless-test

RUN apt-get update -y \
    && env DEBIAN_FRONTEND=noninteractive \
        apt-get install --yes --no-install-recommends \
        python3-requests \
        llvm-9

COPY s3downloader /s3downloader

ENV DATASETS="hits visits"

COPY run.sh /
CMD ["/bin/bash", "/run.sh"]
