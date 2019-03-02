FROM yandex/clickhouse-stateful-test

RUN apt-get update -y \
    && env DEBIAN_FRONTEND=noninteractive \
        apt-get install --yes --no-install-recommends \
            python-requests

COPY s3downloader /s3downloader
COPY run.sh /run.sh

ENV OPEN_DATASETS="hits"
ENV PRIVATE_DATASETS="hits_100m_single hits_10m_single"
ENV DOWNLOAD_DATASETS=1

CMD /run.sh
