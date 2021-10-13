# docker build -t yandex/clickhouse-unit-test .
FROM yandex/clickhouse-stateless-test

RUN apt-get install gdb

COPY run.sh /
COPY process_unit_tests_result.py /
CMD ["/bin/bash", "/run.sh"]
