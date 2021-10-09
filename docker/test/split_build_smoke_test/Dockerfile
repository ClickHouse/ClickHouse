# docker build -t clickhouse/split-build-smoke-test .
FROM clickhouse/binary-builder

COPY run.sh /run.sh
COPY process_split_build_smoke_test_result.py /

CMD /run.sh
