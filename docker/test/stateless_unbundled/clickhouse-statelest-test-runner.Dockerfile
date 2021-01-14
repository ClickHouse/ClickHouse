# Since right now we can't set volumes to the docker during build, we split building container in stages:
# 1. build base container
# 2. run base conatiner with mounted volumes
# 3. commit container as image
FROM ubuntu:18.10 as clickhouse-test-runner-base

# A volume where directory with clickhouse packages to be mounted,
# for later installing.
VOLUME /packages

CMD apt-get update ;\
	DEBIAN_FRONTEND=noninteractive \
	apt install -y /packages/clickhouse-common-static_*.deb \
		/packages/clickhouse-client_*.deb \
		/packages/clickhouse-test_*.deb
