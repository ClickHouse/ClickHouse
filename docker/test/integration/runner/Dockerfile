# docker build -t clickhouse/integration-tests-runner .
FROM ubuntu:22.04

# ARG for quick switch to a given ubuntu mirror
ARG apt_archive="http://archive.ubuntu.com"

RUN sed -i "s|http://archive.ubuntu.com|$apt_archive|g" /etc/apt/sources.list

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
    python3-pika \
    liblua5.1-dev \
    luajit \
    libssl-dev \
    libcurl4-openssl-dev \
    gdb \
    default-jdk \
    software-properties-common \
    libkrb5-dev \
    krb5-user \
    g++ \
    && rm -rf \
        /var/lib/apt/lists/* \
        /var/cache/debconf \
        /tmp/* \
    && apt-get clean

ENV TZ=Etc/UTC
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

ENV DOCKER_CHANNEL stable
# Unpin the docker version after the release 24.0.3 is released
# https://github.com/moby/moby/issues/45770#issuecomment-1618255130
RUN curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add - \
    && add-apt-repository "deb https://download.docker.com/linux/ubuntu $(lsb_release -c -s) ${DOCKER_CHANNEL}" \
    && apt-get update \
    && env DEBIAN_FRONTEND=noninteractive apt-get install --yes \
        docker-ce='5:23.*' \
    && rm -rf \
        /var/lib/apt/lists/* \
        /var/cache/debconf \
        /tmp/* \
    && apt-get clean \
    && dockerd --version; docker --version


RUN python3 -m pip install --no-cache-dir \
    PyMySQL \
    aerospike==11.1.0 \
    asyncio \
    avro==1.10.2 \
    azure-storage-blob \
    cassandra-driver \
    confluent-kafka==1.9.2 \
    delta-spark==2.3.0 \
    dict2xml \
    dicttoxml \
    docker \
    docker-compose==1.29.2 \
    grpcio \
    grpcio-tools \
    kafka-python \
    kazoo \
    lz4 \
    meilisearch==0.18.3 \
    minio \
    nats-py \
    protobuf \
    psycopg2-binary==2.9.6 \
    pyhdfs \
    pymongo==3.11.0 \
    pyspark==3.3.2 \
    pytest \
    pytest-order==1.0.0 \
    pytest-random \
    pytest-repeat \
    pytest-timeout \
    pytest-xdist \
    pytz \
    redis \
    requests-kerberos \
    tzlocal==2.1 \
    retry \
    urllib3

# Hudi supports only spark 3.3.*, not 3.4
RUN curl -fsSL -O https://dlcdn.apache.org/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz \
    && tar xzvf spark-3.3.2-bin-hadoop3.tgz -C / \
    && rm spark-3.3.2-bin-hadoop3.tgz

# download spark and packages
# if you change packages, don't forget to update them in tests/integration/helpers/cluster.py
RUN packages="org.apache.hudi:hudi-spark3.3-bundle_2.12:0.13.0,\
io.delta:delta-core_2.12:2.3.0,\
org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.1.0" \
    && /spark-3.3.2-bin-hadoop3/bin/spark-shell --packages "$packages" > /dev/null \
    && find /root/.ivy2/ -name '*.jar' -exec ln -sf {} /spark-3.3.2-bin-hadoop3/jars/ \;

RUN set -x \
  && addgroup --system dockremap \
  && adduser --system dockremap \
  && adduser dockremap dockremap \
  && echo 'dockremap:165536:65536' >> /etc/subuid \
  && echo 'dockremap:165536:65536' >> /etc/subgid

COPY modprobe.sh /usr/local/bin/modprobe
COPY dockerd-entrypoint.sh /usr/local/bin/
COPY compose/ /compose/
COPY misc/ /misc/


# Same options as in test/base/Dockerfile
# (in case you need to override them in tests)
ENV TSAN_OPTIONS='halt_on_error=1 history_size=7 memory_limit_mb=46080 second_deadlock_stack=1'
ENV UBSAN_OPTIONS='print_stacktrace=1'
ENV MSAN_OPTIONS='abort_on_error=1 poison_in_dtor=1'

EXPOSE 2375
ENTRYPOINT ["dockerd-entrypoint.sh"]
# To pass additional arguments (i.e. list of tests) use PYTEST_ADDOPTS
CMD ["sh", "-c", "pytest"]
