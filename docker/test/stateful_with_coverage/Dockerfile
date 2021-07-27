# docker build -t yandex/clickhouse-stateful-test-with-coverage .
FROM yandex/clickhouse-stateless-test-with-coverage

COPY s3downloader /s3downloader
COPY run.sh /run.sh

ENV DATASETS="hits visits"

CMD ["/bin/bash", "/run.sh"]

