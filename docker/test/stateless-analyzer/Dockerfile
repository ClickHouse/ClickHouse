ARG FROM_TAG=latest
FROM clickhouse/stateless-test:$FROM_TAG

COPY run.sh /
COPY broken_tests.txt /

CMD ["/bin/bash", "/run.sh"]
