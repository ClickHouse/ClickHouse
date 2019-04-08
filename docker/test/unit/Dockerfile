FROM yandex/clickhouse-stateless-test

ENV TZ=Europe/Moscow
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

CMD ln -s /usr/lib/llvm-8/bin/llvm-symbolizer /usr/bin/llvm-symbolizer; \
    echo "TSAN_OPTIONS='halt_on_error=1 history_size=7'" >> /etc/environment; \
    echo "UBSAN_OPTIONS='print_stacktrace=1'" >> /etc/environment; \
    echo "ASAN_SYMBOLIZER_PATH=/usr/lib/llvm-6.0/bin/llvm-symbolizer" >> /etc/environment; \
    echo "UBSAN_SYMBOLIZER_PATH=/usr/lib/llvm-6.0/bin/llvm-symbolizer" >> /etc/environment; \
    echo "TSAN_SYMBOLIZER_PATH=/usr/lib/llvm-8/bin/llvm-symbolizer" >> /etc/environment; \
    echo "LLVM_SYMBOLIZER_PATH=/usr/lib/llvm-6.0/bin/llvm-symbolizer" >> /etc/environment; \
    service zookeeper start && sleep 7 && /usr/share/zookeeper/bin/zkCli.sh -server localhost:2181 -create create /clickhouse_test ''; \
    /unit_tests_dbms | tee test_output/test_result.txt
