# docker build -t clickhouse/integration-test-with-unity-catalog .
ARG FROM_TAG=latest
FROM clickhouse/integration-test:$FROM_TAG

RUN apt-get update && env DEBIAN_FRONTEND=noninteractive apt-get -y install openjdk-17-jdk-headless && update-alternatives --config java && update-alternatives --config javac

# Clone unity catalog and hack their sample data because it contains inconsistent DeltaLake definition
# Columns marked as nullable while they are not nullable and catalog shows them as non-nullable
RUN git clone https://github.com/unitycatalog/unitycatalog.git && cd unitycatalog && \
            git checkout dece8ecab6e452b09abb8b86483d5f0d67b1e675 && \
            build/sbt -mem 4096 package && \
            sed -i 's/\\"nullable\\":true/\\"nullable\\":false/g' etc/data/external/unity/default/tables/user_countries/_delta_log/00000000000000000000.json

RUN curl -fsSL -O https://archive.apache.org/dist/spark/spark-3.5.4/spark-3.5.4-bin-hadoop3.tgz \
    && tar xzvf spark-3.5.4-bin-hadoop3.tgz -C / \
    && rm spark-3.5.4-bin-hadoop3.tgz

# download spark and packages
# if you change packages, don't forget to update them in tests/integration/helpers/cluster.py
RUN packages="org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-spark_2.12:3.2.1,io.unitycatalog:unitycatalog-spark_2.12:0.2.0" \
    /spark-3.5.4-bin-hadoop3/bin/spark-shell --packages "$packages" > /dev/null \
    && find /root/.ivy2/ -name '*.jar' -exec ln -sf {} /spark-3.5.4-bin-hadoop3/jars/ \;
