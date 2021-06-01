# docker build -t yandex/clickhouse-test-base .
FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive LLVM_VERSION=11

RUN apt-get update \
    && apt-get install ca-certificates lsb-release wget gnupg apt-transport-https \
        --yes --no-install-recommends --verbose-versions \
    && export LLVM_PUBKEY_HASH="bda960a8da687a275a2078d43c111d66b1c6a893a3275271beedf266c1ff4a0cdecb429c7a5cccf9f486ea7aa43fd27f" \
    && wget -nv -O /tmp/llvm-snapshot.gpg.key https://apt.llvm.org/llvm-snapshot.gpg.key \
    && echo "${LLVM_PUBKEY_HASH} /tmp/llvm-snapshot.gpg.key" | sha384sum -c \
    && apt-key add /tmp/llvm-snapshot.gpg.key \
    && export CODENAME="$(lsb_release --codename --short | tr 'A-Z' 'a-z')" \
    && echo "deb [trusted=yes] http://apt.llvm.org/${CODENAME}/ llvm-toolchain-${CODENAME}-${LLVM_VERSION} main" >> \
        /etc/apt/sources.list

# initial packages
RUN apt-get update \
    && apt-get install \
        bash \
        fakeroot \
        ccache \
        curl \
        software-properties-common \
        --yes --no-install-recommends

# Special dpkg-deb (https://github.com/ClickHouse-Extras/dpkg) version which is able
# to compress files using pigz (https://zlib.net/pigz/) instead of gzip.
# Significantly increase deb packaging speed and compatible with old systems
RUN curl -O https://clickhouse-builds.s3.yandex.net/utils/1/dpkg-deb \
    && chmod +x dpkg-deb \
    && cp dpkg-deb /usr/bin

RUN apt-get update \
    && apt-get install \
        clang-${LLVM_VERSION} \
        debhelper \
        devscripts \
        gdb  \
        git \
        gperf \
        lcov \
        llvm-${LLVM_VERSION} \
        moreutils \
        perl \
        pigz \
        pkg-config \
        tzdata \
        --yes --no-install-recommends

# Sanitizer options for services (clickhouse-server)
RUN echo "TSAN_OPTIONS='verbosity=1000 halt_on_error=1 history_size=7'" >> /etc/environment; \
  echo "UBSAN_OPTIONS='print_stacktrace=1'" >> /etc/environment; \
  echo "MSAN_OPTIONS='abort_on_error=1 poison_in_dtor=1'" >> /etc/environment; \
  echo "LSAN_OPTIONS='suppressions=/usr/share/clickhouse-test/config/lsan_suppressions.txt'" >> /etc/environment; \
  ln -s /usr/lib/llvm-${LLVM_VERSION}/bin/llvm-symbolizer /usr/bin/llvm-symbolizer;
# Sanitizer options for current shell (not current, but the one that will be spawned on "docker run")
# (but w/o verbosity for TSAN, otherwise test.reference will not match)
ENV TSAN_OPTIONS='halt_on_error=1 history_size=7'
ENV UBSAN_OPTIONS='print_stacktrace=1'
ENV MSAN_OPTIONS='abort_on_error=1 poison_in_dtor=1'

CMD sleep 1
