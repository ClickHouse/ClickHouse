# docker build -t clickhouse/unit-test .
FROM clickhouse/stateless-test

RUN apt-get install gdb

COPY run.sh /
COPY process_unit_tests_result.py /
CMD ["/bin/bash", "/run.sh"]
