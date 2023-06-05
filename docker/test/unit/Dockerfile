# rebuild in #33610
# docker build -t clickhouse/unit-test .
ARG FROM_TAG=latest
ARG DOCKER_REPO=docker.io
FROM $DOCKER_REPO/clickhouse/stateless-test:$FROM_TAG

RUN apt-get install gdb

COPY run.sh /
COPY process_unit_tests_result.py /
CMD ["/bin/bash", "/run.sh"]
