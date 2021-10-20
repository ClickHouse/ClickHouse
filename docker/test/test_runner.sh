#!/bin/sh

set -e -x

# Not sure why shellcheck complains that rc is not assigned before it is referenced.
# shellcheck disable=SC2154
trap 'rc=$?; echo EXITED WITH: $rc; exit $rc' EXIT

# CLI option to prevent rebuilding images, just re-run tests with images leftover from previuos time
readonly NO_REBUILD_FLAG="--no-rebuild"

readonly CLICKHOUSE_DOCKER_DIR="$(realpath "${1}")"
readonly CLICKHOUSE_PACKAGES_ARG="${2}"
CLICKHOUSE_SERVER_IMAGE="${3}"

if [ "${CLICKHOUSE_PACKAGES_ARG}" != "${NO_REBUILD_FLAG}" ]; then
    readonly CLICKHOUSE_PACKAGES_DIR="$(realpath "${2}")" # or --no-rebuild
fi


# In order to allow packages directory to be anywhere, and to reduce amount of context sent to the docker daemon,
# all images are built in multiple stages:
# 1. build base image, install dependencies
# 2. run image with volume mounted, install what needed from those volumes
# 3. tag container as image
# 4. [optional] build another image atop of tagged.

# TODO: optionally mount most recent clickhouse-test and queries directory from local machine

if [ "${CLICKHOUSE_PACKAGES_ARG}" != "${NO_REBUILD_FLAG}" ]; then
    docker build --network=host \
        -f "${CLICKHOUSE_DOCKER_DIR}/test/stateless/clickhouse-statelest-test-runner.Dockerfile" \
        --target clickhouse-test-runner-base \
        -t clickhouse-test-runner-base:preinstall \
        "${CLICKHOUSE_DOCKER_DIR}/test/stateless"

    docker rm -f clickhouse-test-runner-installing-packages || true
    docker run  --network=host \
        -v "${CLICKHOUSE_PACKAGES_DIR}:/packages" \
        --name clickhouse-test-runner-installing-packages \
        clickhouse-test-runner-base:preinstall
    docker commit clickhouse-test-runner-installing-packages clickhouse-statelest-test-runner:local
    docker rm -f clickhouse-test-runner-installing-packages || true
fi

# # Create a bind-volume to the clickhouse-test script file
# docker volume create --driver local --opt type=none --opt device=/home/enmk/proj/ClickHouse_master/tests/clickhouse-test --opt o=bind clickhouse-test-script-volume
# docker volume create --driver local --opt type=none --opt device=/home/enmk/proj/ClickHouse_master/tests/queries --opt o=bind clickhouse-test-queries-dir-volume

# Build server image (optional) from local packages
if [ -z "${CLICKHOUSE_SERVER_IMAGE}" ]; then
    CLICKHOUSE_SERVER_IMAGE="clickhouse/server:local"

    if [ "${CLICKHOUSE_PACKAGES_ARG}" != "${NO_REBUILD_FLAG}" ]; then
        docker build --network=host \
            -f "${CLICKHOUSE_DOCKER_DIR}/server/local.Dockerfile" \
            --target clickhouse-server-base \
            -t clickhouse-server-base:preinstall \
            "${CLICKHOUSE_DOCKER_DIR}/server"

        docker rm -f clickhouse_server_base_installing_server || true
        docker run  --network=host -v "${CLICKHOUSE_PACKAGES_DIR}:/packages" \
            --name clickhouse_server_base_installing_server \
            clickhouse-server-base:preinstall
        docker commit clickhouse_server_base_installing_server clickhouse-server-base:postinstall

        docker build --network=host \
            -f "${CLICKHOUSE_DOCKER_DIR}/server/local.Dockerfile" \
            --target clickhouse-server \
            -t "${CLICKHOUSE_SERVER_IMAGE}" \
            "${CLICKHOUSE_DOCKER_DIR}/server"
    fi
fi

docker rm -f test-runner || true
docker-compose down
CLICKHOUSE_SERVER_IMAGE="${CLICKHOUSE_SERVER_IMAGE}" \
    docker-compose -f "${CLICKHOUSE_DOCKER_DIR}/test/test_runner_docker_compose.yaml" \
    create \
    --build --force-recreate

CLICKHOUSE_SERVER_IMAGE="${CLICKHOUSE_SERVER_IMAGE}" \
    docker-compose -f "${CLICKHOUSE_DOCKER_DIR}/test/test_runner_docker_compose.yaml" \
    run \
    --name test-runner \
    test-runner
