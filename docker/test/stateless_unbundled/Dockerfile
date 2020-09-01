# docker build -t yandex/clickhouse-stateless-unbundled-test .
FROM ubuntu:20.04

ARG odbc_driver_url="https://github.com/ClickHouse/clickhouse-odbc/releases/download/v1.1.4.20200302/clickhouse-odbc-1.1.4-Linux.tar.gz"

ENV DEBIAN_FRONTEND=noninteractive LLVM_VERSION=10

RUN apt-get update \
    && apt-get install ca-certificates lsb-release wget gnupg apt-transport-https \
        --yes --no-install-recommends --verbose-versions \
    && export LLVM_PUBKEY_HASH="bda960a8da687a275a2078d43c111d66b1c6a893a3275271beedf266c1ff4a0cdecb429c7a5cccf9f486ea7aa43fd27f" \
    && wget -O /tmp/llvm-snapshot.gpg.key https://apt.llvm.org/llvm-snapshot.gpg.key \
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
        perl \
        pigz \
        pkg-config \
        tzdata \
                    alien \
            brotli \
            cmake \
            devscripts \
            expect \
            gdb  \
            gperf \
            gperf \
            heimdal-multidev \
            intel-opencl-icd \
            libboost-filesystem-dev \
            libboost-iostreams-dev \
            libboost-program-options-dev \
            libboost-regex-dev \
            libboost-system-dev \
            libboost-thread-dev \
            libc++-dev \
            libc++abi-dev \
            libcapnp-dev \
            libcctz-dev \
            libcurl4-openssl-dev \
            libdouble-conversion-dev \
            libgoogle-perftools-dev \
            libhyperscan-dev \
            libicu-dev \
            libjemalloc-dev \
            libldap2-dev \
            libltdl-dev \
            liblz4-dev \
            libmsgpack-dev \
            libpoco-dev \
            libpoconetssl62 \
            librdkafka-dev \
            libre2-dev \
            libreadline-dev \
            libsasl2-dev \
            libzstd-dev \
            lsof \
            moreutils \
            ncdu \
            netcat-openbsd \
            ocl-icd-libopencl1 \
            odbcinst \
            opencl-headers \
            openssl \
            perl \
            pigz \
            pkg-config \
            python \
            python-lxml \
            qemu-user-static \
            sudo \
            telnet \
            tree \
            tzdata \
            unixodbc \
            unixodbc-dev \
            wget \
            zlib1g-dev \
            zookeeper \
            zookeeperd \
            --yes --no-install-recommends



RUN mkdir -p /tmp/clickhouse-odbc-tmp \
   && wget --quiet -O - ${odbc_driver_url} | tar --strip-components=1 -xz -C /tmp/clickhouse-odbc-tmp \
   && cp /tmp/clickhouse-odbc-tmp/lib64/*.so /usr/local/lib/ \
   && odbcinst -i -d -f /tmp/clickhouse-odbc-tmp/share/doc/clickhouse-odbc/config/odbcinst.ini.sample \
   && odbcinst -i -s -l -f /tmp/clickhouse-odbc-tmp/share/doc/clickhouse-odbc/config/odbc.ini.sample \
   && rm -rf /tmp/clickhouse-odbc-tmp

# Sanitizer options
RUN echo "TSAN_OPTIONS='verbosity=1000 halt_on_error=1 history_size=7'" >> /etc/environment; \
  echo "UBSAN_OPTIONS='print_stacktrace=1'" >> /etc/environment; \
  echo "MSAN_OPTIONS='abort_on_error=1'" >> /etc/environment; \
  ln -s /usr/lib/llvm-${LLVM_VERSION}/bin/llvm-symbolizer /usr/bin/llvm-symbolizer;


ENV TZ=Europe/Moscow
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

COPY run.sh /
CMD ["/bin/bash", "/run.sh"]
