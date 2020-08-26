# docker build -t yandex/clickhouse-test-base .
FROM ubuntu:19.10

RUN apt-get --allow-unauthenticated update -y && apt-get install --yes wget gnupg
RUN wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add -
RUN echo "deb [trusted=yes] http://apt.llvm.org/eoan/ llvm-toolchain-eoan-10 main" >> /etc/apt/sources.list

# initial packages
RUN apt-get --allow-unauthenticated update -y \
    && env DEBIAN_FRONTEND=noninteractive \
        apt-get --allow-unauthenticated install --yes --no-install-recommends \
            apt-transport-https \
            bash \
            ca-certificates \
            curl \
            fakeroot \
            gnupg \
            software-properties-common

# Special dpkg-deb (https://github.com/ClickHouse-Extras/dpkg) version which is able
# to compress files using pigz (https://zlib.net/pigz/) instead of gzip.
# Significantly increase deb packaging speed and compatible with old systems
RUN curl -O https://clickhouse-builds.s3.yandex.net/utils/1/dpkg-deb
RUN chmod +x dpkg-deb
RUN cp dpkg-deb /usr/bin

RUN apt-get --allow-unauthenticated update -y \
    && env DEBIAN_FRONTEND=noninteractive \
        apt-get --allow-unauthenticated install --yes --no-install-recommends \
            clang-10 \
            debhelper \
            devscripts \
            gdb  \
            git \
            gperf \
            lcov \
            llvm-10 \
            moreutils \
            perl \
            perl \
            pigz \
            pkg-config \
            tzdata

# Sanitizer options
RUN echo "TSAN_OPTIONS='verbosity=1000 halt_on_error=1 history_size=7'" >> /etc/environment; \
  echo "UBSAN_OPTIONS='print_stacktrace=1'" >> /etc/environment; \
  echo "MSAN_OPTIONS='abort_on_error=1'" >> /etc/environment; \
  ln -s /usr/lib/llvm-10/bin/llvm-symbolizer /usr/bin/llvm-symbolizer;

CMD sleep 1