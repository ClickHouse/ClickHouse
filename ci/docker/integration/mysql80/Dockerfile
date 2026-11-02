# docker build -t clickhouse/test-mysql80 .

FROM ubuntu/mysql:8.0-22.04_edge

USER root

RUN apt-get update \
    && apt-get install -y iptables iproute2 curl \
    && rm -rf /var/lib/apt/lists/*



WORKDIR /testapp
