# docker build -t yandex/clickhouse-coverage .
ARG PARENT_TAG=latest
FROM yandex/clickhouse-test-base:${PARENT_TAG}

ENV COVERAGE_DIR=/coverage_reports
ENV SOURCE_DIR=/build
ENV OUTPUT_DIR=/output
ENV IGNORE='.*contrib.*'

CMD mkdir -p /build/obj-x86_64-linux-gnu && cd /build/obj-x86_64-linux-gnu && CC=clang-10 CXX=clang++-10 cmake .. && cd /; \
    dpkg -i /package_folder/clickhouse-common-static_*.deb; \
    llvm-profdata-10 merge -sparse ${COVERAGE_DIR}/* -o clickhouse.profdata && \
    llvm-cov-10 export /usr/bin/clickhouse -instr-profile=clickhouse.profdata -j=16 -format=lcov -skip-functions -ignore-filename-regex $IGNORE > output.lcov && \
    genhtml output.lcov --ignore-errors source --output-directory ${OUTPUT_DIR}
