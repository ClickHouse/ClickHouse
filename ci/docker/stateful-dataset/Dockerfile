# docker build -t clickhouse/clickhouse-stateful-test .
#
# Bakes the hits_v1/visits_v1 stateful datasets into a local, self-contained
# (table_disk) store at /opt/ch-stateful. clickhouse/stateless-test is built
# FROM this image, so functional-test env setup attaches these tables instantly
# from a local disk (see tests/docker_scripts/create.sql) instead of lazily
# reading them from the web disk (real AWS S3), which is too slow.
#
# This image is rebuilt only when this directory changes (generate.sh /
# create_source.sql), so the ~GiB dataset layer is stable and cached by the
# in-region dockerhub pull-through proxy.
ARG FROM_TAG=latest
FROM clickhouse/test-base:$FROM_TAG

COPY create_source.sql generate.sh /opt/gen/
RUN bash /opt/gen/generate.sh && rm -rf /opt/gen
