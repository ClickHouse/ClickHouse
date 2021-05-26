# docker build -t yandex/clickhouse-unit-test .
FROM yandex/clickhouse-stateless-test

ENV TZ=Europe/Moscow
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
RUN apt-get install gdb

COPY run.sh /
COPY process_unit_tests_result.py /
CMD ["/bin/bash", "/run.sh"]
