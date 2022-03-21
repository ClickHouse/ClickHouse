# rebuild in #33610
# docker build -t clickhouse/split-build-smoke-test .
ARG FROM_TAG=latest
FROM clickhouse/binary-builder:$FROM_TAG

COPY run.sh /run.sh
COPY process_split_build_smoke_test_result.py /

CMD /run.sh
