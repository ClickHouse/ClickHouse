# docker build -t yandex/clickhouse-unit-test .
FROM yandex/clickhouse-stateless-test

ENV TZ=Europe/Moscow
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
RUN apt-get install gdb

CMD service zookeeper start && sleep 7 && /usr/share/zookeeper/bin/zkCli.sh -server localhost:2181 -create create /clickhouse_test ''; \
    gdb -q  -ex 'set print inferior-events off' -ex 'set confirm off' -ex 'set print thread-events off' -ex run -ex bt -ex quit --args ./unit_tests_dbms | tee test_output/test_result.txt
