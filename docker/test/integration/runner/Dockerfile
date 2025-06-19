# docker build -t clickhouse/integration-tests-runner .
ARG FROM_TAG=latest
FROM clickhouse/test-util:$FROM_TAG

# ARG for quick switch to a given ubuntu mirror
ARG apt_archive="https://archive.ubuntu.com"

RUN sed -i "s|https://archive.ubuntu.com|$apt_archive|g" /etc/apt/sources.list

# We need ca-certificates first to be able to update all repos. This is why it's run twice
RUN apt-get update \
    && apt-get install ca-certificates --yes --no-install-recommends --verbose-versions

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
    python3-pip \
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
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*

ENV TZ=Etc/UTC
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

ENV DOCKER_CHANNEL stable
RUN curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add - \
    && add-apt-repository "deb https://download.docker.com/linux/ubuntu $(lsb_release -c -s) ${DOCKER_CHANNEL}" \
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
RUN python3 -m pip install --no-cache-dir -r requirements.txt

RUN curl -fsSL -O https://archive.apache.org/dist/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz \
    && tar xzvf spark-3.5.5-bin-hadoop3.tgz -C / \
    && rm spark-3.5.5-bin-hadoop3.tgz

# download spark and packages
# if you change packages, don't forget to update them in tests/integration/helpers/cluster.py
RUN packages="io.delta:delta-spark_2.12:3.1.0,\
org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.1,\
org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3" \
    && /spark-3.5.5-bin-hadoop3/bin/spark-shell --packages "$packages" > /dev/null \
    && find /root/.ivy2/ -name '*.jar' -exec ln -sf {} /spark-3.5.5-bin-hadoop3/jars/ \;

RUN set -x \
  && addgroup --system dockremap \
  && adduser --system dockremap \
  && adduser dockremap dockremap \
  && echo 'dockremap:165536:65536' >> /etc/subuid \
  && echo 'dockremap:165536:65536' >> /etc/subgid

COPY modprobe.sh /usr/local/bin/modprobe
COPY dockerd-entrypoint.sh /usr/local/bin/
COPY misc/ /misc/

COPY --from=clickhouse/cctools:425d6927d1efbd790bdb /opt/gdb /opt/gdb
ENV PATH="/opt/gdb/bin:${PATH}"

COPY --from=clickhouse/cctools:425d6927d1efbd790bdb \
  /opt/openssl-fips/openssl.cnf \
  /opt/openssl-fips/fipsmodule.cnf \
  /opt/openssl-fips/fips.so \
  \
  /etc/ssl/

# Same options as in test/base/Dockerfile
# (in case you need to override them in tests)
ENV TSAN_OPTIONS='halt_on_error=1 abort_on_error=1 history_size=7 memory_limit_mb=46080 second_deadlock_stack=1'
ENV UBSAN_OPTIONS='print_stacktrace=1'
ENV MSAN_OPTIONS='abort_on_error=1 poison_in_dtor=1'

EXPOSE 2375
ENTRYPOINT ["dockerd-entrypoint.sh"]
# To pass additional arguments (i.e. list of tests) use PYTEST_ADDOPTS
CMD ["sh", "-c", "pytest"]
