ARG FROM_TAG=latest
FROM clickhouse/test-base:$FROM_TAG

ENV TZ=Europe/Amsterdam
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

COPY *.sh /
COPY *.sql /

CMD ["/bin/bash", "/run.sh"]
