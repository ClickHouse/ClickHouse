# docker build -t clickhouse/integration-tests-runner .
ARG FROM_TAG=latest
FROM clickhouse/test-base:$FROM_TAG

# ARG for quick switch to a given ubuntu mirror
ARG apt_archive="http://archive.ubuntu.com"

# We need ca-certificates first to be able to update all repos. This is why it's run twice
RUN rm -rf /etc/apt/sources.list.d/*.list \
    && apt-get update -o Acquire::Check-Valid-Until=false || apt-get update --allow-insecure-repositories \
    && apt-get install ca-certificates --yes --no-install-recommends --verbose-versions || \
       (apt-get install ca-certificates --yes --no-install-recommends --verbose-versions --allow-unauthenticated)

RUN sed -i "s|https://archive.ubuntu.com|$apt_archive|g" /etc/apt/sources.list

RUN apt-get update \
    && env DEBIAN_FRONTEND=noninteractive apt-get install --yes \
    adduser \
    ca-certificates \
    bash \
    btrfs-progs \
    e2fsprogs \
    iptables \
    xfsprogs \
    tar \
    pigz \
    wget \
    git \
    iproute2 \
    cgroupfs-mount \
    tzdata \
    libicu-dev \
    bsdutils \
    curl \
    openssh-client \
    liblua5.1-dev \
    luajit \
    libssl-dev \
    libcurl4-openssl-dev \
    default-jdk \
    software-properties-common \
    libkrb5-dev \
    krb5-user \
    g++ \
    awscli \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*

# clang, for the `CI Tests` job. `ci/tests/test_jemalloc_safety_checks_cmake.py` answers
# whether the jemalloc safety-check macros are armed by *compiling* a probe rather than by
# parsing a compile line, so without a compiler its whole oracle half skips - and praktika
# counts a skip as success, leaving that guard reporting success without measuring
# anything. That module now fails instead of skipping when the compiler is missing in CI
# (see `_require_compiler_or_skip`), so this image has to carry one.
#
# The LLVM apt source cannot be inherited: test-base adds it as
# /etc/apt/sources.list.d/llvm.list, and line 9 above removes
# /etc/apt/sources.list.d/*.list before its own first `apt-get update`. The signing key
# does survive (`apt-key add` writes /etc/apt/trusted.gpg, not a *.list), so only the
# source line has to be restored. LLVM_VERSION and lsb-release both come from test-base.
RUN export CODENAME="$(lsb_release --codename --short | tr 'A-Z' 'a-z')" \
    && echo "deb https://apt.llvm.org/${CODENAME}/ llvm-toolchain-${CODENAME}-${LLVM_VERSION} main" > /etc/apt/sources.list.d/llvm.list \
    && apt-get update \
    && env DEBIAN_FRONTEND=noninteractive apt-get install --yes --no-install-recommends \
        clang-${LLVM_VERSION} \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/* \
    && clang-${LLVM_VERSION} --version

ENV TZ=Etc/UTC
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

ENV DOCKER_CHANNEL stable
RUN curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add - \
    && echo "deb [arch=$(dpkg --print-architecture)] https://download.docker.com/linux/ubuntu $(lsb_release -cs) ${DOCKER_CHANNEL}" > /etc/apt/sources.list.d/docker.list \
    && apt-get update \
    && env DEBIAN_FRONTEND=noninteractive apt-get install --yes \
        docker-ce="5:27.0.3*" \
        docker-compose-plugin='2.29.*' \
    && rm -rf \
        /var/lib/apt/lists/* \
        /var/cache/debconf \
        /tmp/* \
    && apt-get clean \
    && dockerd --version; docker --version

# kazoo 2.10.0 is broken
# https://s3.amazonaws.com/clickhouse-test-reports/59337/524625a1d2f4cc608a3f1059e3df2c30f353a649/integration_tests__asan__analyzer__[5_6].html
COPY requirements.txt /
RUN python3.13 -m pip install --no-cache-dir -r requirements.txt


RUN curl -fsSL -O https://archive.apache.org/dist/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz \
    && tar xzvf spark-3.5.5-bin-hadoop3.tgz -C / \
    && rm spark-3.5.5-bin-hadoop3.tgz

# download spark and packages
# if you change packages, don't forget to update them in tests/integration/helpers/cluster.py
RUN packages="io.delta:delta-spark_2.12:3.1.0,\
org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.1,\
org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,\
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.262,\
org.apache.hadoop:hadoop-azure:3.3.4,\
com.microsoft.azure:azure-storage:8.6.6,\
org.apache.spark:spark-avro_2.12:3.5.1,\
org.apache.sedona:sedona-spark-3.5_2.12:1.6.1"\
    && /spark-3.5.5-bin-hadoop3/bin/spark-shell --packages "$packages" \
    && find /root/.ivy2/ -name '*.jar' -exec ln -sf {} /spark-3.5.5-bin-hadoop3/jars/ \;

RUN set -x \
  && addgroup --system dockremap \
  && adduser --system dockremap \
  && adduser dockremap dockremap \
  && echo 'dockremap:165536:65536' >> /etc/subuid \
  && echo 'dockremap:165536:65536' >> /etc/subgid

COPY modprobe.sh /usr/local/bin/modprobe
COPY misc/ /misc/

COPY --from=clickhouse/cctools:859fb360308eb8ac47ad /opt/gdb /opt/gdb
ENV PATH="/opt/gdb/bin:${PATH}"

COPY --from=clickhouse/cctools:859fb360308eb8ac47ad \
  /opt/openssl-fips/openssl.cnf \
  /opt/openssl-fips/fipsmodule.cnf \
  /opt/openssl-fips/fips.so \
  \
  /etc/ssl/

EXPOSE 2375

ENV SPARK_HOME="/spark-3.5.5-bin-hadoop3" \
    PATH="${SPARK_HOME}/bin:${PATH}" \
    JAVA_TOOL_OPTIONS="-Djdk.attach.allowAttachSelf=true"
