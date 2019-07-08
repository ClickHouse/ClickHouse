# docker build -t yandex/clickhouse-stateless-with-coverage-test .
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
            lsof


ENV TZ=Europe/Moscow
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
ENV LLVM_PROFILE_FILE='code_%h_%p_%m.profraw'


CMD chmod 777 /; \
    echo "LLVM_PROFILE_FILE='code_%h_%p_%m.profraw'" >> /etc/environment; \
    dpkg -i package_folder/clickhouse-common-static_*.deb; \
    dpkg -i package_folder/clickhouse-common-static-dbg_*.deb; \
    dpkg -i package_folder/clickhouse-server_*.deb;  \
    dpkg -i package_folder/clickhouse-client_*.deb; \
    dpkg -i package_folder/clickhouse-test_*.deb; \
    ln -s /usr/share/clickhouse-test/config/zookeeper.xml /etc/clickhouse-server/config.d/; \
    ln -s /usr/share/clickhouse-test/config/listen.xml /etc/clickhouse-server/config.d/; \
    ln -s /usr/share/clickhouse-test/config/part_log.xml /etc/clickhouse-server/config.d/; \
    ln -s /usr/share/clickhouse-test/config/log_queries.xml /etc/clickhouse-server/users.d/; \
    ln -s /usr/share/clickhouse-test/config/readonly.xml /etc/clickhouse-server/users.d/; \
    ln -s /usr/share/clickhouse-test/config/ints_dictionary.xml /etc/clickhouse-server/; \
    ln -s /usr/share/clickhouse-test/config/strings_dictionary.xml /etc/clickhouse-server/; \
    ln -s /usr/share/clickhouse-test/config/decimals_dictionary.xml /etc/clickhouse-server/; \
    ln -s /usr/lib/llvm-8/bin/llvm-symbolizer /usr/bin/llvm-symbolizer; \
    service zookeeper start; sleep 5; \
    LLVM_PROFILE_FILE='code_%h_%p_%m.profraw' sudo -Eu clickhouse /usr/bin/clickhouse-server --config /etc/clickhouse-server/config.xml 1>/dev/null 2>/dev/null & \
    sleep 10; \
    LLVM_PROFILE_FILE='client.profraw' clickhouse-test --shard --zookeeper $ADDITIONAL_OPTIONS $SKIP_TESTS_OPTION 2>&1 | ts '%Y-%m-%d %H:%M:%S' | tee test_output/test_result.txt; \
    while kill -0 `pgrep -u clickhouse`; do kill `pgrep -u clickhouse` 2>/dev/null; echo "Process" `pgrep -u clickhouse` "still alive"; sleep 10; done; \
    cp /*.profraw /profraw ||:
