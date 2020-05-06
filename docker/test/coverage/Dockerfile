# docker build -t yandex/clickhouse-coverage .
FROM yandex/clickhouse-deb-builder

RUN apt-get --allow-unauthenticated update -y \
    && env DEBIAN_FRONTEND=noninteractive \
    apt-get --allow-unauthenticated install --yes --no-install-recommends \
    bash \
    fakeroot \
    cmake \
    ccache \
    curl \
    software-properties-common


RUN echo "deb [trusted=yes] http://apt.llvm.org/bionic/ llvm-toolchain-bionic-9 main" >> /etc/apt/sources.list

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


CMD mkdir -p /build/obj-x86_64-linux-gnu && cd /build/obj-x86_64-linux-gnu && CC=clang-9 CXX=clang++-9 cmake .. && cd /; \
    dpkg -i /package_folder/clickhouse-common-static_*.deb; \
    llvm-profdata-9 merge -sparse ${COVERAGE_DIR}/* -o clickhouse.profdata && \
    llvm-cov-9 export /usr/bin/clickhouse -instr-profile=clickhouse.profdata -j=16 -format=lcov -skip-functions -ignore-filename-regex $IGNORE > output.lcov && \
    genhtml output.lcov --ignore-errors source --output-directory ${OUTPUT_DIR}
