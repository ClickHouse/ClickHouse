# rebuild in #33609
# docker build -t clickhouse/docs-release .
ARG FROM_TAG=latest
FROM clickhouse/docs-builder:$FROM_TAG

COPY run.sh /

ENV REPO_PATH=/repo_path
ENV OUTPUT_PATH=/output_path

CMD ["/bin/bash", "/run.sh"]
