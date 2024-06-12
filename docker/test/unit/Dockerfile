# rebuild in #33610
# docker build -t clickhouse/unit-test .
ARG FROM_TAG=latest
FROM clickhouse/test-base:$FROM_TAG

COPY run.sh /
COPY process_unit_tests_result.py /
CMD ["/bin/bash", "/run.sh"]
