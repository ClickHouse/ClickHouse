#!/bin/bash

USAGE='Usage for local run:

./docker/test/stateless/setup_minio.sh { stateful | stateless } ./tests/

'

set -e -x -a -u

TEST_TYPE="$1"
shift

case $TEST_TYPE in
  stateless) QUERY_DIR=0_stateless ;;
  stateful) QUERY_DIR=1_stateful ;;
  *) echo "unknown test type $TEST_TYPE"; echo "${USAGE}"; exit 1 ;;
esac

ls -lha

mkdir -p ./minio_data

if [ ! -f ./minio ]; then
  MINIO_SERVER_VERSION=${MINIO_SERVER_VERSION:-2022-01-03T18-22-58Z}
  MINIO_CLIENT_VERSION=${MINIO_CLIENT_VERSION:-2022-01-05T23-52-51Z}
  case $(uname -m) in
    x86_64) BIN_ARCH=amd64 ;;
    aarch64) BIN_ARCH=arm64 ;;
    *) echo "unknown architecture $(uname -m)"; exit 1 ;;
  esac
  echo 'MinIO binary not found, downloading...'

  BINARY_TYPE=$(uname -s | tr '[:upper:]' '[:lower:]')

  wget "https://dl.min.io/server/minio/release/${BINARY_TYPE}-${BIN_ARCH}/archive/minio.RELEASE.${MINIO_SERVER_VERSION}" -O ./minio \
    && wget "https://dl.min.io/client/mc/release/${BINARY_TYPE}-${BIN_ARCH}/archive/mc.RELEASE.${MINIO_CLIENT_VERSION}" -O ./mc \
    && chmod +x ./mc ./minio
fi

MINIO_ROOT_USER=${MINIO_ROOT_USER:-clickhouse}
MINIO_ROOT_PASSWORD=${MINIO_ROOT_PASSWORD:-clickhouse}

./minio --version
./minio server --address ":11111" ./minio_data &

i=0
while ! curl -v --silent http://localhost:11111 2>&1 | grep AccessDenied
do
  if [[ $i == 60 ]]; then
    echo "Failed to setup minio"
    exit 0
  fi
  echo "Trying to connect to minio"
  sleep 1
  i=$((i + 1))
done

lsof -i :11111

sleep 5

./mc alias set clickminio http://localhost:11111 clickhouse clickhouse
./mc admin user add clickminio test testtest
./mc admin policy set clickminio readwrite user=test
./mc mb clickminio/test
if [ "$TEST_TYPE" = "stateless" ]; then
  ./mc policy set public clickminio/test
fi


# Upload data to Minio. By default after unpacking all tests will in
# /usr/share/clickhouse-test/queries

TEST_PATH=${1:-/usr/share/clickhouse-test}
MINIO_DATA_PATH=${TEST_PATH}/queries/${QUERY_DIR}/data_minio

# Iterating over globs will cause redudant FILE variale to be a path to a file, not a filename
# shellcheck disable=SC2045
for FILE in $(ls "${MINIO_DATA_PATH}"); do
    echo "$FILE";
    ./mc cp "${MINIO_DATA_PATH}"/"$FILE" clickminio/test/"$FILE";
done

mkdir -p ~/.aws
cat <<EOT >> ~/.aws/credentials
[default]
aws_access_key_id=${MINIO_ROOT_USER}
aws_secret_access_key=${MINIO_ROOT_PASSWORD}
EOT
