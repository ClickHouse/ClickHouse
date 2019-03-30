FROM yandex/clickhouse-stateless-test

RUN apt-get update -y \
    && env DEBIAN_FRONTEND=noninteractive \
        apt-get install --yes --no-install-recommends \
        python-requests \
        llvm-8

COPY s3downloader /s3downloader

ENV DATASETS="hits visits"

CMD dpkg -i package_folder/clickhouse-common-static_*.deb; \
    dpkg -i package_folder/clickhouse-common-static-dbg_*.deb; \
    dpkg -i package_folder/clickhouse-server_*.deb;  \
    dpkg -i package_folder/clickhouse-client_*.deb; \
    dpkg -i package_folder/clickhouse-test_*.deb; \
    ln -s /usr/lib/llvm-8/bin/llvm-symbolizer /usr/bin/llvm-symbolizer; \
    echo "TSAN_OPTIONS='halt_on_error=1 history_size=7'" >> /etc/environment; \
    echo "TSAN_SYMBOLIZER_PATH=/usr/lib/llvm-8/bin/llvm-symbolizer" >> /etc/environment; \
    echo "UBSAN_OPTIONS='print_stacktrace=1'" >> /etc/environment; \
    echo "ASAN_SYMBOLIZER_PATH=/usr/lib/llvm-6.0/bin/llvm-symbolizer" >> /etc/environment; \
    echo "UBSAN_SYMBOLIZER_PATH=/usr/lib/llvm-6.0/bin/llvm-symbolizer" >> /etc/environment; \
    echo "LLVM_SYMBOLIZER_PATH=/usr/lib/llvm-6.0/bin/llvm-symbolizer" >> /etc/environment; \
    service zookeeper start; sleep 5; \
    service clickhouse-server start && sleep 5 \
    && /s3downloader --dataset-names $DATASETS \
    && chmod 777 -R /var/lib/clickhouse \
    && clickhouse-client --query "SHOW DATABASES" \
    && clickhouse-client --query "CREATE DATABASE datasets" \
    && clickhouse-client --query "CREATE DATABASE test" \
    && service clickhouse-server restart && sleep 5 \
    && clickhouse-client --query "SHOW TABLES FROM datasets" \
    && clickhouse-client --query "SHOW TABLES FROM test" \
    && clickhouse-client --query "RENAME TABLE datasets.hits_v1 TO test.hits" \
    && clickhouse-client --query "RENAME TABLE datasets.visits_v1 TO test.visits" \
    && clickhouse-client --query "SHOW TABLES FROM test" \
    && clickhouse-test --shard --zookeeper --no-stateless $SKIP_TESTS_OPTION 2>&1 | ts '%Y-%m-%d %H:%M:%S' | tee test_output/test_result.txt
