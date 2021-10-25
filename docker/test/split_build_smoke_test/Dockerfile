# docker build -t yandex/clickhouse-split-build-smoke-test .
FROM yandex/clickhouse-binary-builder

COPY run.sh /run.sh
COPY process_split_build_smoke_test_result.py /

CMD /run.sh
