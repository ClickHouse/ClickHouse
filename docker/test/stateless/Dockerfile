# docker build -t yandex/clickhouse-stateless-test .
FROM yandex/clickhouse-deb-builder

ARG odbc_driver_url="https://github.com/ClickHouse/clickhouse-odbc/releases/download/v1.1.4.20200302/clickhouse-odbc-1.1.4-Linux.tar.gz"

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
            ncdu \
            netcat-openbsd \
            telnet \
            tree \
            moreutils \
            brotli \
            gdb \
            lsof \
            llvm-9 \
            unixodbc \
            wget \
            qemu-user-static

RUN mkdir -p /tmp/clickhouse-odbc-tmp \
   && wget --quiet -O - ${odbc_driver_url} | tar --strip-components=1 -xz -C /tmp/clickhouse-odbc-tmp \
   && cp /tmp/clickhouse-odbc-tmp/lib64/*.so /usr/local/lib/ \
   && odbcinst -i -d -f /tmp/clickhouse-odbc-tmp/share/doc/clickhouse-odbc/config/odbcinst.ini.sample \
   && odbcinst -i -s -l -f /tmp/clickhouse-odbc-tmp/share/doc/clickhouse-odbc/config/odbc.ini.sample \
   && rm -rf /tmp/clickhouse-odbc-tmp

ENV TZ=Europe/Moscow
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Sanitizer options
RUN echo "TSAN_OPTIONS='verbosity=1000 halt_on_error=1 history_size=7'" >> /etc/environment; \
    echo "UBSAN_OPTIONS='print_stacktrace=1'" >> /etc/environment; \
    echo "MSAN_OPTIONS='abort_on_error=1'" >> /etc/environment; \
    ln -s /usr/lib/llvm-9/bin/llvm-symbolizer /usr/bin/llvm-symbolizer;

CMD dpkg -i package_folder/clickhouse-common-static_*.deb; \
    dpkg -i package_folder/clickhouse-common-static-dbg_*.deb; \
    dpkg -i package_folder/clickhouse-server_*.deb;  \
    dpkg -i package_folder/clickhouse-client_*.deb; \
    dpkg -i package_folder/clickhouse-test_*.deb; \
    mkdir -p /etc/clickhouse-server/dict_examples; \
    ln -s /usr/share/clickhouse-test/config/ints_dictionary.xml /etc/clickhouse-server/dict_examples/; \
    ln -s /usr/share/clickhouse-test/config/strings_dictionary.xml /etc/clickhouse-server/dict_examples/; \
    ln -s /usr/share/clickhouse-test/config/decimals_dictionary.xml /etc/clickhouse-server/dict_examples/; \
    ln -s /usr/share/clickhouse-test/config/zookeeper.xml /etc/clickhouse-server/config.d/; \
    ln -s /usr/share/clickhouse-test/config/listen.xml /etc/clickhouse-server/config.d/; \
    ln -s /usr/share/clickhouse-test/config/part_log.xml /etc/clickhouse-server/config.d/; \
    ln -s /usr/share/clickhouse-test/config/text_log.xml /etc/clickhouse-server/config.d/; \
    ln -s /usr/share/clickhouse-test/config/metric_log.xml /etc/clickhouse-server/config.d/; \
    ln -s /usr/share/clickhouse-test/config/query_masking_rules.xml /etc/clickhouse-server/config.d/; \
    ln -s /usr/share/clickhouse-test/config/log_queries.xml /etc/clickhouse-server/users.d/; \
    ln -s /usr/share/clickhouse-test/config/readonly.xml /etc/clickhouse-server/users.d/; \
    ln -s /usr/share/clickhouse-test/config/access_management.xml /etc/clickhouse-server/users.d/; \
    ln -s /usr/share/clickhouse-test/config/ints_dictionary.xml /etc/clickhouse-server/; \
    ln -s /usr/share/clickhouse-test/config/strings_dictionary.xml /etc/clickhouse-server/; \
    ln -s /usr/share/clickhouse-test/config/decimals_dictionary.xml /etc/clickhouse-server/; \
    ln -s /usr/share/clickhouse-test/config/macros.xml /etc/clickhouse-server/config.d/; \
    ln -s /usr/share/clickhouse-test/config/disks.xml /etc/clickhouse-server/config.d/; \
    ln -s /usr/share/clickhouse-test/config/secure_ports.xml /etc/clickhouse-server/config.d/; \
    ln -s /usr/share/clickhouse-test/config/clusters.xml /etc/clickhouse-server/config.d/; \
    ln -s /usr/share/clickhouse-test/config/graphite.xml /etc/clickhouse-server/config.d/; \
    ln -s /usr/share/clickhouse-test/config/server.key /etc/clickhouse-server/; \
    ln -s /usr/share/clickhouse-test/config/server.crt /etc/clickhouse-server/; \
    ln -s /usr/share/clickhouse-test/config/dhparam.pem /etc/clickhouse-server/; \
    if [[ -n "$USE_POLYMORPHIC_PARTS" ]] && [[ "$USE_POLYMORPHIC_PARTS" -eq 1 ]]; then ln -s /usr/share/clickhouse-test/config/polymorphic_parts.xml /etc/clickhouse-server/config.d/; fi; \
    if [[ -n "$USE_DATABASE_ATOMIC" ]] && [[ "$USE_DATABASE_ATOMIC" -eq 1 ]]; then ln -s /usr/share/clickhouse-test/config/database_atomic_configd.xml /etc/clickhouse-server/config.d/; fi; \
    if [[ -n "$USE_DATABASE_ATOMIC" ]] && [[ "$USE_DATABASE_ATOMIC" -eq 1 ]]; then ln -s /usr/share/clickhouse-test/config/database_atomic_usersd.xml /etc/clickhouse-server/users.d/; fi; \
    ln -sf /usr/share/clickhouse-test/config/client_config.xml /etc/clickhouse-client/config.xml; \
    service zookeeper start; sleep 5; \
    service clickhouse-server start && sleep 5 && clickhouse-test --testname --shard --zookeeper --use-skip-list $ADDITIONAL_OPTIONS $SKIP_TESTS_OPTION 2>&1 | ts '%Y-%m-%d %H:%M:%S' | tee test_output/test_result.txt
