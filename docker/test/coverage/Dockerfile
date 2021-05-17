FROM yandex/clickhouse-stateless-test

RUN apt-get update -y \
    && env DEBIAN_FRONTEND=noninteractive \
        apt-get install --yes --no-install-recommends \
            clang-tools-11 \
            cmake \
            c++filt

COPY s3downloader /s3downloader

COPY ccr_converter.py /ccr_converter.py
COPY run.sh /run.sh

ENV DATASETS="hits visits"
ENV SOURCE_DIR=/build

ENV OUTPUT_DIR=/output
ENV GENHTML_REPORT_DIR=/output/report

ENV IGNORE='.*contrib.*'

CMD ["/bin/bash", "/run.sh"]
