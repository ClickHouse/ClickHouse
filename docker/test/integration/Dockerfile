# docker build -t yandex/clickhouse-integration-test .
FROM ubuntu:18.04

RUN echo "deb [trusted=yes] http://apt.llvm.org/bionic/ llvm-toolchain-bionic-8 main" >> /etc/apt/sources.list

RUN apt-get update \
    && env DEBIAN_FRONTEND=noninteractive apt-get -y install tzdata python llvm-6.0 llvm-6.0-dev libreadline-dev libicu-dev bsdutils llvm-8 gdb \
    && rm -rf \
        /var/lib/apt/lists/* \
        /var/cache/debconf \
        /tmp/* \
    && apt-get clean

ENV TZ=Europe/Moscow
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN echo "TSAN_OPTIONS='halt_on_error=1 history_size=7'" >> /etc/environment; \
    echo "UBSAN_OPTIONS='print_stacktrace=1'" >> /etc/environment; \
    echo "ASAN_SYMBOLIZER_PATH=/usr/lib/llvm-6.0/bin/llvm-symbolizer" >> /etc/environment; \
    echo "TSAN_SYMBOLIZER_PATH=/usr/lib/llvm-8/bin/llvm-symbolizer" >> /etc/environment; \
    echo "UBSAN_SYMBOLIZER_PATH=/usr/lib/llvm-6.0/bin/llvm-symbolizer" >> /etc/environment; \
    echo "LLVM_SYMBOLIZER_PATH=/usr/lib/llvm-6.0/bin/llvm-symbolizer" >> /etc/environment;
