# rebuild in #33610
# docker build -t clickhouse/test-base .
ARG FROM_TAG=latest
FROM clickhouse/test-util:$FROM_TAG

RUN apt-get update \
    && apt-get install \
        lcov \
        netbase \
        perl \
        pv \
        ripgrep \
        zstd \
        locales \
        sudo \
        --yes --no-install-recommends \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*


# Sanitizer options for services (clickhouse-server)
# Set resident memory limit for TSAN to 45GiB (46080MiB) to avoid OOMs in Stress tests
# and MEMORY_LIMIT_EXCEEDED exceptions in Functional tests (total memory limit in Functional tests is ~55.24 GiB).
# TSAN will flush shadow memory when reaching this limit.
# It may cause false-negatives, but it's better than OOM.
#  max_allocation_size_mb is set to 32GB, so we have much bigger chance to run into memory limit than the limitation of the sanitizers
RUN echo "TSAN_OPTIONS='verbosity=1000 halt_on_error=1 abort_on_error=1 history_size=7 memory_limit_mb=46080 second_deadlock_stack=1 max_allocation_size_mb=32768'" >> /etc/environment
RUN echo "UBSAN_OPTIONS='print_stacktrace=1 max_allocation_size_mb=32768'" >> /etc/environment
RUN echo "MSAN_OPTIONS='abort_on_error=1 poison_in_dtor=1 max_allocation_size_mb=32768'" >> /etc/environment
RUN echo "LSAN_OPTIONS='suppressions=/usr/share/clickhouse-test/config/lsan_suppressions.txt max_allocation_size_mb=32768'" >> /etc/environment
RUN echo "ASAN_OPTIONS='halt_on_error=1 abort_on_error=1'" >> /etc/environment
# Sanitizer options for current shell (not current, but the one that will be spawned on "docker run")
# (but w/o verbosity for TSAN, otherwise test.reference will not match)
ENV TSAN_OPTIONS='halt_on_error=1 abort_on_error=1 history_size=7 memory_limit_mb=46080 second_deadlock_stack=1 max_allocation_size_mb=32768'
ENV UBSAN_OPTIONS='print_stacktrace=1 max_allocation_size_mb=32768'
ENV MSAN_OPTIONS='abort_on_error=1 poison_in_dtor=1 max_allocation_size_mb=32768'
ENV LSAN_OPTIONS='max_allocation_size_mb=32768'
ENV ASAN_OPTIONS='halt_on_error=1 abort_on_error=1'

# for external_symbolizer_path, and also ensure that llvm-symbolizer really
# exists (since you don't want to fallback to addr2line, it is very slow)
RUN test -f /usr/bin/llvm-symbolizer-${LLVM_VERSION}
RUN ln -s /usr/bin/llvm-symbolizer-${LLVM_VERSION} /usr/bin/llvm-symbolizer

RUN echo "en_US.UTF-8 UTF-8" > /etc/locale.gen && locale-gen en_US.UTF-8
ENV LC_ALL en_US.UTF-8

ENV TZ=Europe/Amsterdam
RUN ln -snf "/usr/share/zoneinfo/$TZ" /etc/localtime && echo "$TZ" > /etc/timezone

# This script is used to setup realtime export of server logs from the CI into external ClickHouse cluster:
COPY setup_export_logs.sh /

CMD sleep 1
