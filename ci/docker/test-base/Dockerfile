# docker build -t clickhouse/test-base .
FROM ubuntu:22.04

# ARG for quick switch to a given ubuntu mirror
ARG apt_archive="http://archive.ubuntu.com"
RUN sed -i -e "s|http://archive.ubuntu.com|$apt_archive|g" -e "s|https://archive.ubuntu.com|$apt_archive|g" /etc/apt/sources.list
ARG LLVM_APT_VERSION="1:21.1.5~"

ENV DEBIAN_FRONTEND=noninteractive LLVM_VERSION=21

# We need ca-certificates first to be able to update all repos. This is why it's run twice
RUN apt-get update \
    && apt-get install ca-certificates --yes --no-install-recommends --verbose-versions

RUN apt-get update \
    && apt-get install \
        libc6-dbg \
        sudo \
        apt-transport-https \
        apt-utils \
        curl \
        dnsutils \
        gnupg \
        file \
        iputils-ping \
        lsb-release \
        xxd \
        wget \
        iptables \
        iproute2 \
        gh \
        --yes --no-install-recommends --verbose-versions \
    && export LLVM_PUBKEY_HASH="5ffc7c9a9299ce774f81cada703e23ebba5bdfb0345b6c3b667b3ead7aa21c75ef62ccd74f7a8f2aa0cbe158d3068bbe" \
    && wget -nv -O /tmp/llvm-snapshot.gpg.key https://apt.llvm.org/llvm-snapshot.gpg.key \
    && echo "${LLVM_PUBKEY_HASH} /tmp/llvm-snapshot.gpg.key" | sha384sum -c \
    && apt-key add /tmp/llvm-snapshot.gpg.key \
    && export CODENAME="$(lsb_release --codename --short | tr 'A-Z' 'a-z')" \
    && echo "deb https://apt.llvm.org/${CODENAME}/ llvm-toolchain-${CODENAME}-${LLVM_VERSION} main" >> /etc/apt/sources.list.d/llvm.list \
    && apt-get update \
    && apt-get satisfy --yes --no-install-recommends "llvm-${LLVM_VERSION} (>= ${LLVM_APT_VERSION})" \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*

# for external_symbolizer_path, and also ensure that llvm-symbolizer really
# exists (since you don't want to fallback to addr2line, it is very slow)
RUN test -f /usr/bin/llvm-symbolizer-${LLVM_VERSION}
RUN ln -s /usr/bin/llvm-symbolizer-${LLVM_VERSION} /usr/bin/llvm-symbolizer

# Install LLVM tools for code coverage (llvm-profdata, llvm-cov)
# Must match LLVM_VERSION used for building (21)
RUN export CODENAME="$(lsb_release --codename --short | tr 'A-Z' 'a-z')" \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        llvm-${LLVM_VERSION} \
        make \
        libdatetime-perl \
        libcapture-tiny-perl \
        libjson-xs-perl \
        libperlio-gzip-perl \
    && ln -s /usr/bin/llvm-profdata-${LLVM_VERSION} /usr/bin/llvm-profdata \
    && ln -s /usr/bin/llvm-cov-${LLVM_VERSION} /usr/bin/llvm-cov \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*

# Install newer lcov from source
# Ubuntu 22.04 has old 1.14 version, we need 2.x for --sort-tables and --hierarchical options
RUN cd /tmp \
    && wget -q https://github.com/linux-test-project/lcov/releases/download/v2.4/lcov-2.4.tar.gz \
    && tar xzf lcov-2.4.tar.gz \
    && cd lcov-2.4 \
    && make install \
    && cd /tmp \
    && rm -rf lcov-2.4 lcov-2.4.tar.gz

# Install cmake 3.20+ for Rust support
# Used https://askubuntu.com/a/1157132 as reference
RUN curl -s https://apt.kitware.com/keys/kitware-archive-latest.asc | gpg --dearmor - > /etc/apt/trusted.gpg.d/kitware.gpg && \
    echo "deb [signed-by=/etc/apt/trusted.gpg.d/kitware.gpg] https://apt.kitware.com/ubuntu/ $(lsb_release -cs) main" >> /etc/apt/sources.list.d/kitware.list

RUN apt-get update \
    && apt-get install \
        bash \
        bsdmainutils \
        build-essential \
        git \
        openssh-client \
        gperf \
        moreutils \
        less \
        nasm \
        pigz \
        rename \
        software-properties-common \
        tzdata \
        netbase \
        perl \
        pv \
        python3-requests \
        ripgrep \
        zstd \
        locales \
        libmpfr6 \
        --yes --no-install-recommends \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*

# Note, libmpfr6 is also a requirement for gdb
COPY --from=clickhouse/cctools:859fb360308eb8ac47ad /opt/gdb /opt/gdb
ENV PATH="/opt/gdb/bin:${PATH}"

COPY --from=clickhouse/cctools:859fb360308eb8ac47ad \
  /opt/openssl-fips/openssl.cnf \
  /opt/openssl-fips/fipsmodule.cnf \
  /opt/openssl-fips/fips.so \
  \
  /etc/ssl/

# Install ODBC Bridge and Library Bridge, which are separately built components.
RUN BRIDGE_VERSION='25.1.5.31' \
    && ARCH=$(dpkg --print-architecture) \
    && cd /tmp \
    && wget "https://packages.clickhouse.com/deb/pool/main/c/clickhouse/clickhouse-odbc-bridge_${BRIDGE_VERSION}_${ARCH}.deb" \
    && wget "https://packages.clickhouse.com/deb/pool/main/c/clickhouse/clickhouse-library-bridge_${BRIDGE_VERSION}_${ARCH}.deb" \
    && dpkg -i clickhouse-odbc-bridge_${BRIDGE_VERSION}_${ARCH}.deb clickhouse-library-bridge_${BRIDGE_VERSION}_${ARCH}.deb \
    && rm clickhouse-odbc-bridge_${BRIDGE_VERSION}_${ARCH}.deb clickhouse-library-bridge_${BRIDGE_VERSION}_${ARCH}.deb

COPY jeprof /usr/bin/jeprof
RUN wget -q -O /usr/bin/jeprof.orig https://raw.githubusercontent.com/jemalloc/jemalloc/41a859ef7325569c6c25f92d294d45123bb81355/bin/jeprof.in &&  \
    sed -i -e 's/@jemalloc_version@/clickhouse/g' -e 's/@JEMALLOC_PREFIX@//g' /usr/bin/jeprof.orig && chmod +x /usr/bin/jeprof.orig

RUN wget -q -O /usr/bin/flamegraph.pl https://raw.githubusercontent.com/brendangregg/FlameGraph/cd9ee4c4449775a2f867acf31c84b7fe4b132ad5/flamegraph.pl && chmod +x /usr/bin/flamegraph.pl
RUN wget -q -O /usr/bin/difffolded.pl https://raw.githubusercontent.com/brendangregg/FlameGraph/cd9ee4c4449775a2f867acf31c84b7fe4b132ad5/difffolded.pl && chmod +x /usr/bin/difffolded.pl

RUN apt-key adv --keyserver keyserver.ubuntu.com --recv-keys F23C5A6CF475977595C89F51BA6932366A755776 \
    && echo "deb https://ppa.launchpadcontent.net/deadsnakes/ppa/ubuntu $(lsb_release -cs) main" > /etc/apt/sources.list.d/deadsnakes-ppa.list \
    && printf 'Package: *\nPin: release o=LP-PPA-deadsnakes\nPin-Priority: 100\n' > /etc/apt/preferences.d/deadsnakes \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        python3.13 \
        python3.13-venv \
        python3.13-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/* \
    && curl -sS https://bootstrap.pypa.io/get-pip.py | python3.13

RUN echo "en_US.UTF-8 UTF-8" > /etc/locale.gen && locale-gen en_US.UTF-8
ENV LC_ALL en_US.UTF-8

ENV TZ=Europe/Amsterdam
RUN ln -snf "/usr/share/zoneinfo/$TZ" /etc/localtime && echo "$TZ" > /etc/timezone

# Make python3.13 the default python3 so CI scripts run with the right interpreter.
RUN ln -sf /usr/bin/python3.13 /usr/bin/python3

# Set a default USER so that Python's getpass.getuser() doesn't raise OSError
# when the container is run with --user uid:gid and the UID is not in /etc/passwd.
# Python 3.13 changed getpass.getuser() to raise OSError instead of KeyError in that case.
ENV USER=nobody
