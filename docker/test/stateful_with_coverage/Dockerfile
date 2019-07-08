# docker build -t yandex/clickhouse-stateful-test .
FROM yandex/clickhouse-stateless-test

RUN apt-get update -y \
    && env DEBIAN_FRONTEND=noninteractive \
        apt-get install --yes --no-install-recommends \
        python-requests \
        llvm-8

COPY s3downloader /s3downloader
COPY run.sh /run.sh

ENV DATASETS="hits visits"

CMD ["/bin/bash", "/run.sh"]

