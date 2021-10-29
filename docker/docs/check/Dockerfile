# docker build -t clickhouse/docs-check .
FROM clickhouse/docs-builder

COPY run.sh /

ENV REPO_PATH=/repo_path
ENV OUTPUT_PATH=/output_path

CMD ["/bin/bash", "/run.sh"]
