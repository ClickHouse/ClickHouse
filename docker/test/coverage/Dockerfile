FROM yandex/clickhouse-stateless-test

RUN apt-get update -y \
    && env DEBIAN_FRONTEND=noninteractive \
        apt-get install --yes --no-install-recommends \
            cmake \
    && pip3 install \
            jinja2  \
            pygments \
            tqdm

COPY s3downloader /s3downloader
COPY templates/ /templates/

COPY run.sh /run.sh
COPY ccr_converter.py /ccr_converter.py

ENV DATASETS="hits visits"
ENV SOURCE_DIR=/build

ENV OUTPUT_DIR=/output

ENV IGNORE='.*contrib.*'

CMD ["/bin/bash", "/run.sh"]
