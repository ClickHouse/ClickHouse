# docker build -t yandex/clickhouse-stress-test .
FROM yandex/clickhouse-stateful-test

RUN apt-get update -y \
    && env DEBIAN_FRONTEND=noninteractive \
        apt-get install --yes --no-install-recommends \
            bash \
            tzdata \
            fakeroot \
            debhelper \
            parallel \
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
            llvm-9 \
            brotli

COPY ./stress /stress

ENV DATASETS="hits visits"

CMD dpkg -i package_folder/clickhouse-common-static_*.deb; \
    dpkg -i package_folder/clickhouse-common-static-dbg_*.deb; \
    dpkg -i package_folder/clickhouse-server_*.deb;  \
    dpkg -i package_folder/clickhouse-client_*.deb; \
    dpkg -i package_folder/clickhouse-test_*.deb; \
    ln -s /usr/share/clickhouse-test/config/log_queries.xml /etc/clickhouse-server/users.d/; \
    ln -s /usr/share/clickhouse-test/config/part_log.xml /etc/clickhouse-server/config.d/; \
    ln -s /usr/lib/llvm-9/bin/llvm-symbolizer /usr/bin/llvm-symbolizer; \
    echo "TSAN_OPTIONS='halt_on_error=1 history_size=7 ignore_noninstrumented_modules=1 verbosity=1'" >> /etc/environment; \
    echo "UBSAN_OPTIONS='print_stacktrace=1'" >> /etc/environment; \
    echo "ASAN_OPTIONS='malloc_context_size=10 verbosity=1 allocator_release_to_os_interval_ms=10000'" >> /etc/environment; \
    service clickhouse-server start && sleep 5 \
    && /s3downloader --dataset-names $DATASETS \
    && chmod 777 -R /var/lib/clickhouse \
    && clickhouse-client --query "ATTACH DATABASE IF NOT EXISTS datasets ENGINE = Ordinary" \
    && clickhouse-client --query "CREATE DATABASE IF NOT EXISTS test" \
    && service clickhouse-server restart && sleep 5 \
    && clickhouse-client --query "SHOW TABLES FROM datasets" \
    && clickhouse-client --query "SHOW TABLES FROM test" \
    && clickhouse-client --query "RENAME TABLE datasets.hits_v1 TO test.hits" \
    && clickhouse-client --query "RENAME TABLE datasets.visits_v1 TO test.visits" \
    && clickhouse-client --query "SHOW TABLES FROM test" \
    && ./stress --output-folder test_output
