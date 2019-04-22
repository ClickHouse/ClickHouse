FROM yandex/clickhouse-deb-builder

RUN apt-get update -y \
    && env DEBIAN_FRONTEND=noninteractive \
        apt-get install --yes --no-install-recommends \
            bash \
            tzdata \
            fakeroot \
            debhelper \
            zookeeper \
            zookeeperd \
            expect \
            python \
            python-lxml \
            python-termcolor \
            python-requests \
            curl \
            sudo \
            openssl \
            netcat-openbsd \
            telnet \
            moreutils \
            brotli \
            gdb \
            lsof \
            llvm-8


ENV TZ=Europe/Moscow
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

COPY zookeeper.xml /etc/clickhouse-server/config.d/zookeeper.xml
COPY listen.xml /etc/clickhouse-server/config.d/listen.xml
COPY part_log.xml /etc/clickhouse-server/config.d/part_log.xml
COPY log_queries.xml /etc/clickhouse-server/users.d/log_queries.xml
COPY readonly.xml /etc/clickhouse-server/users.d/readonly.xml

CMD dpkg -i package_folder/clickhouse-common-static_*.deb; \
    dpkg -i package_folder/clickhouse-common-static-dbg_*.deb; \
    dpkg -i package_folder/clickhouse-server_*.deb;  \
    dpkg -i package_folder/clickhouse-client_*.deb; \
    dpkg -i package_folder/clickhouse-test_*.deb; \
    ln -s /usr/lib/llvm-8/bin/llvm-symbolizer /usr/bin/llvm-symbolizer; \
    echo "TSAN_OPTIONS='halt_on_error=1 history_size=7'" >> /etc/environment; \
    echo "UBSAN_OPTIONS='print_stacktrace=1'" >> /etc/environment; \
    echo "ASAN_SYMBOLIZER_PATH=/usr/lib/llvm-6.0/bin/llvm-symbolizer" >> /etc/environment; \
    echo "UBSAN_SYMBOLIZER_PATH=/usr/lib/llvm-6.0/bin/llvm-symbolizer" >> /etc/environment; \
    echo "TSAN_SYMBOLIZER_PATH=/usr/lib/llvm-8/bin/llvm-symbolizer" >> /etc/environment; \
    echo "LLVM_SYMBOLIZER_PATH=/usr/lib/llvm-6.0/bin/llvm-symbolizer" >> /etc/environment; \
    service zookeeper start; sleep 5; \
    service clickhouse-server start && sleep 5 && clickhouse-test --shard --zookeeper $ADDITIONAL_OPTIONS $SKIP_TESTS_OPTION 2>&1 | ts '%Y-%m-%d %H:%M:%S' | tee test_output/test_result.txt
