FROM ubuntu:18.10

ARG CLICKHOUSE_PACKAGES_DIR
COPY ${CLICKHOUSE_PACKAGES_DIR}/clickhouse-*.deb /packages/

RUN apt-get update ;\
	DEBIAN_FRONTEND=noninteractive \
	apt install -y /packages/clickhouse-common-static_*.deb \
		/packages/clickhouse-client_*.deb \
		/packages/clickhouse-test_*.deb \
		wait-for-it; \
	rm -rf /packages
