FROM ubuntu:18.04
# yandex/clickhouse-integration-test

RUN apt-get update \
    && env DEBIAN_FRONTEND=noninteractive apt-get -y install tzdata python llvm-6.0 llvm-6.0-dev libreadline-dev libicu-dev bsdutils \
    && rm -rf \
        /var/lib/apt/lists/* \
        /var/cache/debconf \
        /tmp/* \
    && apt-get clean

ENV TZ=Europe/Moscow
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

ENV TSAN_OPTIONS 'halt_on_error=1 history_size=7'
ENV UBSAN_OPTIONS 'print_stacktrace=1'
ENV ASAN_SYMBOLIZER_PATH /usr/lib/llvm-6.0/bin/llvm-symbolizer
ENV UBSAN_SYMBOLIZER_PATH /usr/lib/llvm-6.0/bin/llvm-symbolizer
ENV LLVM_SYMBOLIZER_PATH /usr/lib/llvm-6.0/bin/llvm-symbolizer