# rebuild in #33610
# docker build -t clickhouse/unit-test .
ARG FROM_TAG=latest
FROM clickhouse/stateless-test:$FROM_TAG

RUN apt-get install gdb

COPY run.sh /
CMD ["/bin/bash", "/run.sh"]
