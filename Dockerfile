FROM docker.intuit.com/docker-rmt/bitnami/clickhouse:24-debian-12
COPY programs/clickhouse /opt/bitnami/clickhouse/bin/clickhouse
