#!/bin/bash

# TODO: Make this file shared with stateless tests
#
# Usage for local run:
#
# ./docker/test/stateful/setup_minio.sh ./tests/
#

set -e -x -a -u

rpm2cpio ./minio-20220103182258.0.0.x86_64.rpm | cpio -i --make-directories
find -name minio
cp ./usr/local/bin/minio ./

ls -lha

mkdir -p ./minio_data

if [ ! -f ./minio ]; then
  echo 'MinIO binary not found, downloading...'

  BINARY_TYPE=$(uname -s | tr '[:upper:]' '[:lower:]')

  wget "https://dl.min.io/server/minio/release/${BINARY_TYPE}-amd64/minio" \
    && chmod +x ./minio \
    && wget "https://dl.min.io/client/mc/release/${BINARY_TYPE}-amd64/mc" \
    && chmod +x ./mc
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


# Upload data to Minio. By default after unpacking all tests will in
# /usr/share/clickhouse-test/queries

TEST_PATH=${1:-/usr/share/clickhouse-test}
MINIO_DATA_PATH=${TEST_PATH}/queries/1_stateful/data_minio

# Iterating over globs will cause redudant FILE variale to be a path to a file, not a filename
# shellcheck disable=SC2045
for FILE in $(ls "${MINIO_DATA_PATH}"); do
    echo "$FILE";
    ./mc cp "${MINIO_DATA_PATH}"/"$FILE" clickminio/test/"$FILE";
done

mkdir -p ~/.aws
cat <<EOT >> ~/.aws/credentials
[default]
aws_access_key_id=clickhouse
aws_secret_access_key=clickhouse
EOT
