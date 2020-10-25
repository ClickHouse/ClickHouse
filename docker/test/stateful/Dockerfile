# docker build -t yandex/clickhouse-stateful-test .
FROM yandex/clickhouse-stateless-test

RUN apt-get update -y \
    && env DEBIAN_FRONTEND=noninteractive \
        apt-get install --yes --no-install-recommends \
        python-requests \
        llvm-9

COPY s3downloader /s3downloader

ENV DATASETS="hits visits"

COPY run.sh /
CMD ["/bin/bash", "/run.sh"]
