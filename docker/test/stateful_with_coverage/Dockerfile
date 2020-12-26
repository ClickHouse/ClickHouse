# docker build -t yandex/clickhouse-stateful-test-with-coverage .
FROM yandex/clickhouse-stateless-test-with-coverage

RUN apt-get update -y \
    && env DEBIAN_FRONTEND=noninteractive \
        apt-get install --yes --no-install-recommends \
        python3-requests procps psmisc

COPY s3downloader /s3downloader
COPY run.sh /run.sh

ENV DATASETS="hits visits"

CMD ["/bin/bash", "/run.sh"]

