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
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/debconf /tmp/*

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


# kazoo 2.10.0 is broken
# https://s3.amazonaws.com/clickhouse-test-reports/59337/524625a1d2f4cc608a3f1059e3df2c30f353a649/integration_tests__asan__analyzer__[5_6].html
RUN python3 -m pip install --no-cache-dir \
    PyMySQL==1.1.0 \
    asyncio==3.4.3 \
    avro==1.10.2 \
    azure-storage-blob==12.19.0 \
    boto3==1.34.24 \
    cassandra-driver==3.29.0 \
    confluent-kafka==2.3.0 \
    delta-spark==2.3.0 \
    dict2xml==1.7.4 \
    dicttoxml==1.7.16 \
    docker==6.1.3 \
    docker-compose==1.29.2 \
    grpcio==1.60.0 \
    grpcio-tools==1.60.0 \
    kafka-python==2.0.2 \
    lz4==4.3.3 \
    minio==7.2.3 \
    nats-py==2.6.0 \
    protobuf==4.25.2 \
    kazoo==2.9.0 \
    psycopg2-binary==2.9.6 \
    pyhdfs==0.3.1 \
    pymongo==3.11.0 \
    pyspark==3.3.2 \
    pytest==7.4.4 \
    pytest-order==1.0.0 \
    pytest-random==0.2 \
    pytest-repeat==0.9.3 \
    pytest-timeout==2.2.0 \
    pytest-xdist==3.5.0 \
    pytest-reportlog==0.4.0 \
    pytz==2023.3.post1 \
    pyyaml==5.3.1 \
    redis==5.0.1 \
    requests-kerberos==0.14.0 \
    tzlocal==2.1 \
    retry==0.9.2 \
    bs4==0.0.2 \
    lxml==5.1.0 \
    urllib3==2.0.7
# bs4, lxml are for cloud tests, do not delete

# Hudi supports only spark 3.3.*, not 3.4
RUN curl -fsSL -O https://archive.apache.org/dist/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz \
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
COPY misc/ /misc/


# Same options as in test/base/Dockerfile
# (in case you need to override them in tests)
ENV TSAN_OPTIONS='halt_on_error=1 abort_on_error=1 history_size=7 memory_limit_mb=46080 second_deadlock_stack=1'
ENV UBSAN_OPTIONS='print_stacktrace=1'
ENV MSAN_OPTIONS='abort_on_error=1 poison_in_dtor=1'

EXPOSE 2375
ENTRYPOINT ["dockerd-entrypoint.sh"]
# To pass additional arguments (i.e. list of tests) use PYTEST_ADDOPTS
CMD ["sh", "-c", "pytest"]
