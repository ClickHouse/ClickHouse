#!/bin/sh

set -e

# Run tests in docker
# OR
# Build containers from deb packages, copying the tests from the source directory 

readonly CLICKHOUSE_DOCKER_DIR="$(realpath ${1})"
readonly CLICKHOUSE_PACKAGES_DIR="${2}"
CLICKHOUSE_SERVER_IMAGE="${3}"

# Build test runner image
docker build \
	-f "${CLICKHOUSE_DOCKER_DIR}/test/stateless/clickhouse-statelest-test-runner.Dockerfile" \
	-t clickhouse-statelest-test-runner:local \
	--build-arg CLICKHOUSE_PACKAGES_DIR="${CLICKHOUSE_PACKAGES_DIR}" \
	"${CLICKHOUSE_DOCKER_DIR}"

# Build server image (optional) from local packages
if [ -z "${CLICKHOUSE_SERVER_IMAGE}" ]; then
	CLICKHOUSE_SERVER_IMAGE="yandex/clickhouse_server:local"

	docker build \
		-f "${CLICKHOUSE_DOCKER_DIR}/server/local.Dockerfile" \
		-t "${CLICKHOUSE_SERVER_IMAGE}" \
		--build-arg CLICKHOUSE_PACKAGES_DIR=${CLICKHOUSE_PACKAGES_DIR} \
		"${CLICKHOUSE_DOCKER_DIR}"
fi

CLICKHOUSE_SERVER_IMAGE="${CLICKHOUSE_SERVER_IMAGE}" docker-compose -f "${CLICKHOUSE_DOCKER_DIR}/test/test_runner_docker_compose.yaml" run test-runner