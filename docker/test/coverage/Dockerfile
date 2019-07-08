# docker build -t yandex/clickhouse-coverage .
FROM ubuntu:18.04

RUN apt-get --allow-unauthenticated update -y \
    && env DEBIAN_FRONTEND=noninteractive \
    apt-get --allow-unauthenticated install --yes --no-install-recommends \
    bash \
    fakeroot \
    cmake \
    ccache \
    curl \
    software-properties-common


RUN echo "deb [trusted=yes] http://apt.llvm.org/bionic/ llvm-toolchain-bionic main" >> /etc/apt/sources.list

RUN apt-get --allow-unauthenticated update -y \
    && env DEBIAN_FRONTEND=noninteractive \
        apt-get --allow-unauthenticated install --yes --no-install-recommends \
            perl \
            lcov \
            llvm-9 \
            tzdata


ENV COVERAGE_DIR=/coverage_reports
ENV SOURCE_DIR=/build
ENV OUTPUT_DIR=/output
ENV IGNORE='.*contrib.*'


CMD dpkg -i /package_folder/clickhouse-common-static_*.deb; \
    llvm-profdata-9 merge -sparse ${COVERAGE_DIR}/* -o clickhouse.profdata && \
    llvm-cov-9 export /usr/bin/clickhouse -instr-profile=clickhouse.profdata -j=16 -format=lcov -skip-functions -ignore-filename-regex $IGNORE > output.lcov && \
    genhtml output.lcov --ignore-errors source --output-directory ${OUTPUT_DIR}
