# docker build -t clickhouse/test-coverage .
FROM clickhouse/stateless-test

RUN apt-get update -y \
    && env DEBIAN_FRONTEND=noninteractive \
        apt-get install --yes --no-install-recommends \
            cmake

COPY s3downloader /s3downloader
COPY run.sh /run.sh

ENV DATASETS="hits visits"
ENV COVERAGE_DIR=/coverage_reports
ENV SOURCE_DIR=/build
ENV OUTPUT_DIR=/output
ENV IGNORE='.*contrib.*'

CMD ["/bin/bash", "/run.sh"]
