# rebuild in #33610
# docker build -t clickhouse/docs-check .
ARG FROM_TAG=latest
FROM clickhouse/docs-builder:$FROM_TAG

COPY run.sh /

ENV REPO_PATH=/repo_path
ENV OUTPUT_PATH=/output_path

CMD ["/bin/bash", "/run.sh"]
