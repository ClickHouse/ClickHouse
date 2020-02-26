# docker build -t yandex/clickhouse-stateless-test .
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
            ncdu \
            netcat-openbsd \
            telnet \
            tree \
            moreutils \
            brotli \
            gdb \
            lsof \
            llvm-8 \
            vim \
            unixodbc \
            wget

ARG cmake_url=https://cmake.org/files/v3.16/cmake-3.16.4-Linux-x86_64.tar.gz

# clickhouse-odbc build & install (also temporary install newer cmake)
RUN mkdir -p /tmp/odbc-driver-build/cmake \
   && wget --no-check-certificate --quiet -O - ${cmake_url} | tar --strip-components=1 -xz -C /tmp/odbc-driver-build/cmake \
   # shallow clone is enough for build, but smaller and clone is faster
   && git clone --depth 1 https://github.com/ClickHouse/clickhouse-odbc.git /tmp/odbc-driver-build/clickhouse-odbc \
   && cd /tmp/odbc-driver-build/clickhouse-odbc \
   # shallow submodules are MUCH smaller and faster
   && git submodule update --init --depth 1  \
   && mkdir build \
   && cd build \
   && /tmp/odbc-driver-build/cmake/bin/cmake --version \
   && /tmp/odbc-driver-build/cmake/bin/cmake .. -DCMAKE_C_COMPILER=`which gcc-9` -DCMAKE_CXX_COMPILER=`which g++-9` -DCH_ODBC_ENABLE_TESTING=OFF \
   && /tmp/odbc-driver-build/cmake/bin/cmake --build . --parallel --target install \
   && odbcinst -i -d -f /usr/local/share/doc/clickhouse-odbc/config/odbcinst.ini.sample \
   && odbcinst -i -s -l -f /usr/local/share/doc/clickhouse-odbc/config/odbc.ini.sample \
   # make resulting docker image smaller:
   && rm -rf /tmp/odbc-driver-build

ENV TZ=Europe/Moscow
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Sanitizer options
RUN echo "TSAN_OPTIONS='verbosity=1000 halt_on_error=1 history_size=7'" >> /etc/environment; \
    echo "UBSAN_OPTIONS='print_stacktrace=1'" >> /etc/environment; \
    echo "MSAN_OPTIONS='abort_on_error=1'" >> /etc/environment; \
    ln -s /usr/lib/llvm-8/bin/llvm-symbolizer /usr/bin/llvm-symbolizer;

CMD dpkg -i package_folder/clickhouse-common-static_*.deb; \
    dpkg -i package_folder/clickhouse-common-static-dbg_*.deb; \
    dpkg -i package_folder/clickhouse-server_*.deb;  \
    dpkg -i package_folder/clickhouse-client_*.deb; \
    dpkg -i package_folder/clickhouse-test_*.deb; \
    ln -s /usr/share/clickhouse-test/config/zookeeper.xml /etc/clickhouse-server/config.d/; \
    ln -s /usr/share/clickhouse-test/config/listen.xml /etc/clickhouse-server/config.d/; \
    ln -s /usr/share/clickhouse-test/config/part_log.xml /etc/clickhouse-server/config.d/; \
    ln -s /usr/share/clickhouse-test/config/text_log.xml /etc/clickhouse-server/config.d/; \
    ln -s /usr/share/clickhouse-test/config/metric_log.xml /etc/clickhouse-server/config.d/; \
    ln -s /usr/share/clickhouse-test/config/query_masking_rules.xml /etc/clickhouse-server/config.d/; \
    ln -s /usr/share/clickhouse-test/config/log_queries.xml /etc/clickhouse-server/users.d/; \
    ln -s /usr/share/clickhouse-test/config/readonly.xml /etc/clickhouse-server/users.d/; \
    ln -s /usr/share/clickhouse-test/config/ints_dictionary.xml /etc/clickhouse-server/; \
    ln -s /usr/share/clickhouse-test/config/strings_dictionary.xml /etc/clickhouse-server/; \
    ln -s /usr/share/clickhouse-test/config/decimals_dictionary.xml /etc/clickhouse-server/; \
    ln -s /usr/share/clickhouse-test/config/macros.xml /etc/clickhouse-server/config.d/; \
    ln -s /usr/share/clickhouse-test/config/disks.xml /etc/clickhouse-server/config.d/; \
    ln -s /usr/share/clickhouse-test/config/secure_ports.xml /etc/clickhouse-server/config.d/; \
    ln -s /usr/share/clickhouse-test/config/server.key /etc/clickhouse-server/; \
    ln -s /usr/share/clickhouse-test/config/server.crt /etc/clickhouse-server/; \
    ln -s /usr/share/clickhouse-test/config/dhparam.pem /etc/clickhouse-server/; \
    ln -sf /usr/share/clickhouse-test/config/client_config.xml /etc/clickhouse-client/config.xml; \
    service zookeeper start; sleep 5; \
    service clickhouse-server start && sleep 5 && clickhouse-test --testname --shard --zookeeper $ADDITIONAL_OPTIONS $SKIP_TESTS_OPTION 2>&1 | ts '%Y-%m-%d %H:%M:%S' | tee test_output/test_result.txt
