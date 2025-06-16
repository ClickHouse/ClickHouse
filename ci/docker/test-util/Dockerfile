# docker build -t clickhouse/test-util .
FROM ubuntu:22.04

# ARG for quick switch to a given ubuntu mirror
ARG apt_archive="https://archive.ubuntu.com"
RUN sed -i -e "s|http://archive.ubuntu.com|$apt_archive|g" -e "s|https://archive.ubuntu.com|$apt_archive|g" /etc/apt/sources.list
ARG LLVM_APT_VERSION="1:19.1.4"

ENV DEBIAN_FRONTEND=noninteractive LLVM_VERSION=19

# We need ca-certificates first to be able to update all repos. This is why it's run twice
RUN apt-get update \
    && apt-get install ca-certificates --yes --no-install-recommends --verbose-versions

RUN apt-get update \
    && apt-get install \
        sudo \
        apt-transport-https \
        apt-utils \
        curl \
        dnsutils \
        gnupg \
        iputils-ping \
        lsb-release \
        xxd \
        wget \
        --yes --no-install-recommends --verbose-versions \
    && export LLVM_PUBKEY_HASH="bda960a8da687a275a2078d43c111d66b1c6a893a3275271beedf266c1ff4a0cdecb429c7a5cccf9f486ea7aa43fd27f" \
    && wget -nv -O /tmp/llvm-snapshot.gpg.key https://apt.llvm.org/llvm-snapshot.gpg.key \
    && echo "${LLVM_PUBKEY_HASH} /tmp/llvm-snapshot.gpg.key" | sha384sum -c \
    && apt-key add /tmp/llvm-snapshot.gpg.key \
    && export CODENAME="$(lsb_release --codename --short | tr 'A-Z' 'a-z')" \
    && echo "deb https://apt.llvm.org/${CODENAME}/ llvm-toolchain-${CODENAME}-${LLVM_VERSION} main" >> /etc/apt/sources.list.d/llvm.list \
    && apt-get update \
    && apt-get satisfy --yes --no-install-recommends "llvm-${LLVM_VERSION} (>= ${LLVM_APT_VERSION})" \
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

# Install cmake 3.20+ for Rust support
# Used https://askubuntu.com/a/1157132 as reference
RUN curl -s https://apt.kitware.com/keys/kitware-archive-latest.asc | gpg --dearmor - > /etc/apt/trusted.gpg.d/kitware.gpg && \
    echo "deb [signed-by=/etc/apt/trusted.gpg.d/kitware.gpg] https://apt.kitware.com/ubuntu/ $(lsb_release -cs) main" >> /etc/apt/sources.list.d/kitware.list

# initial packages
RUN apt-get update \
    && apt-get install \
        bash \
        bsdmainutils \
        build-essential \
        git \
        gperf \
        moreutils \
        nasm \
        pigz \
        rename \
        software-properties-common \
        tzdata \
        --yes --no-install-recommends \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*

# Install ODBC Bridge and Library Bridge, which are separately built components.
RUN BRIDGE_VERSION='25.1.5.31' \
    && ARCH=$(dpkg --print-architecture) \
    && cd /tmp \
    && wget "https://packages.clickhouse.com/deb/pool/main/c/clickhouse/clickhouse-odbc-bridge_${BRIDGE_VERSION}_${ARCH}.deb" \
    && wget "https://packages.clickhouse.com/deb/pool/main/c/clickhouse/clickhouse-library-bridge_${BRIDGE_VERSION}_${ARCH}.deb" \
    && dpkg -i clickhouse-odbc-bridge_${BRIDGE_VERSION}_${ARCH}.deb clickhouse-library-bridge_${BRIDGE_VERSION}_${ARCH}.deb \
    && rm clickhouse-odbc-bridge_${BRIDGE_VERSION}_${ARCH}.deb clickhouse-library-bridge_${BRIDGE_VERSION}_${ARCH}.deb

COPY --from=clickhouse/cctools:425d6927d1efbd790bdb /opt/gdb /opt/gdb
ENV PATH="/opt/gdb/bin:${PATH}"

COPY --from=clickhouse/cctools:425d6927d1efbd790bdb \
  /opt/openssl-fips/openssl.cnf \
  /opt/openssl-fips/fipsmodule.cnf \
  /opt/openssl-fips/fips.so \
  /etc/ssl/

# Additional packages from test-base
RUN apt-get update \
    && apt-get install \
        bsdutils \
        default-jre \
        g++ \
        iproute2 \
        krb5-user \
        libicu-dev \
        libsqlite3-dev \
        libsqliteodbc \
        lsof \
        lz4 \
        odbc-postgresql \
        odbcinst \
        python3-pip \
        libcurl4-openssl-dev \
        libssl-dev \
        rpm2cpio \
        sqlite3 \
        tar \
        unixodbc \
        --yes --no-install-recommends \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*

# Install Python requirements
COPY requirements.txt /
RUN pip3 install --no-cache-dir -r requirements.txt && rm -rf /root/.cache/pip

# Install MySQL ODBC driver from RHEL rpm
ARG TARGETARCH
RUN arch=${TARGETARCH:-amd64} \
  && case $arch in \
      amd64) rarch=x86_64 ;; \
      arm64) rarch=aarch64 ;; \
    esac \
  && cd /tmp \
  && curl -o mysql-odbc.rpm "https://cdn.mysql.com/archives/mysql-connector-odbc-8.0/mysql-connector-odbc-8.0.32-1.el9.${rarch}.rpm" \
  && rpm2archive mysql-odbc.rpm \
  && tar xf mysql-odbc.rpm.tgz -C / ./usr/lib64/ \
  && rm mysql-odbc.rpm mysql-odbc.rpm.tgz \
  && ODBC_DIR=$(dpkg -L odbc-postgresql | grep -E '^/usr/lib/.+-linux-gnu/odbc$') \
  && ln -s /usr/lib64/libmyodbc8a.so "$ODBC_DIR" \
  && ln -s /usr/lib64/libmyodbc8a.so "$ODBC_DIR"/libmyodbc.so

# ZooKeeper installation
ENV ZOOKEEPER_VERSION='3.6.3'
RUN curl "https://archive.apache.org/dist/zookeeper/zookeeper-${ZOOKEEPER_VERSION}/apache-zookeeper-${ZOOKEEPER_VERSION}-bin.tar.gz" | \
      tar -C opt -zxv && \
  mv /opt/apache-zookeeper-${ZOOKEEPER_VERSION}-bin /opt/zookeeper && \
  chmod -R 777 /opt/zookeeper && \
  echo $'tickTime=2500 \n\
tickTime=2500 \n\
dataDir=/zookeeper \n\
clientPort=2181 \n\
maxClientCnxns=80' > /opt/zookeeper/conf/zoo.cfg && \
  mkdir /zookeeper && \
  chmod -R 777 /zookeeper

# Timezone configuration
ENV TZ=Etc/UTC
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
