# rebuild in #33610
# docker build -t clickhouse/unit-test .
ARG FROM_TAG=latest
FROM clickhouse/test-base:$FROM_TAG

COPY run.sh /
RUN chmod +x run.sh
ENTRYPOINT ["/run.sh"]
