#!/bin/bash

set -euxf -o pipefail

export MINIO_ROOT_USER=${MINIO_ROOT_USER:-clickhouse}
export MINIO_ROOT_PASSWORD=${MINIO_ROOT_PASSWORD:-clickhouse}

usage() {
  echo $"Usage: $0 <stateful|stateless> <test_path> (default path: /usr/share/clickhouse-test)"
  exit 1
}

check_arg() {
  local query_dir
  if [ ! $# -eq 1 ]; then
    if [ ! $# -eq 2 ]; then
      echo "ERROR: need either one or two arguments, <stateful|stateless> <test_path> (default path: /usr/share/clickhouse-test)"
      usage
    fi
  fi
  case "$1" in
    stateless)
      query_dir="0_stateless"
      ;;
    stateful)
      query_dir="1_stateful"
      ;;
    *)
      echo "unknown test type ${test_type}"
      usage
      ;;
  esac
  echo ${query_dir}
}

find_arch() {
  local arch
  case $(uname -m) in
    x86_64)
      arch="amd64"
      ;;
    aarch64)
      arch="arm64"
      ;;
    *)
      echo "unknown architecture $(uname -m)";
      exit 1
      ;;
  esac
  echo ${arch}
}

find_os() {
  local os
  os=$(uname -s | tr '[:upper:]' '[:lower:]')
  echo "${os}"
}

download_minio() {
  local os
  local arch
  local minio_server_version=${MINIO_SERVER_VERSION:-2024-08-03T04-33-23Z}
  local minio_client_version=${MINIO_CLIENT_VERSION:-2024-07-31T15-58-33Z}

  os=$(find_os)
  arch=$(find_arch)
  wget "https://dl.min.io/server/minio/release/${os}-${arch}/archive/minio.RELEASE.${minio_server_version}" -O ./minio
  wget "https://dl.min.io/client/mc/release/${os}-${arch}/archive/mc.RELEASE.${minio_client_version}" -O ./mc
  chmod +x ./mc ./minio
}

start_minio() {
  mkdir -p ./minio_data
  ./minio --version
  ./minio server --address ":11111" ./minio_data &
  wait_for_it
  lsof -i :11111
  sleep 5
}

setup_minio() {
  local test_type=$1
  ./mc alias set clickminio http://localhost:11111 clickhouse clickhouse
  ./mc admin user add clickminio test testtest
  ./mc admin policy attach clickminio readwrite --user=test
  ./mc mb --ignore-existing clickminio/test
  if [ "$test_type" = "stateless" ]; then
    ./mc anonymous set public clickminio/test
  fi
}

# uploads data to minio, by default after unpacking all tests
# will be in /usr/share/clickhouse-test/queries
upload_data() {
  local query_dir=$1
  local test_path=$2
  local data_path=${test_path}/queries/${query_dir}/data_minio

  # iterating over globs will cause redundant file variable to be
  # a path to a file, not a filename
  # shellcheck disable=SC2045
  if [ -d "${data_path}" ]; then
    ./mc cp --recursive "${data_path}"/ clickminio/test/
  fi
}

setup_aws_credentials() {
  local minio_root_user=${MINIO_ROOT_USER:-clickhouse}
  local minio_root_password=${MINIO_ROOT_PASSWORD:-clickhouse}
  mkdir -p ~/.aws
  cat <<EOT >> ~/.aws/credentials
[default]
aws_access_key_id=${minio_root_user}
aws_secret_access_key=${minio_root_password}
EOT
}

wait_for_it() {
  local counter=0
  local max_counter=60
  local url="http://localhost:11111"
  local params=(
    --silent
    --verbose
  )
  while ! curl "${params[@]}" "${url}" 2>&1 | grep AccessDenied
  do
    if [[ ${counter} == "${max_counter}" ]]; then
      echo "failed to setup minio"
      exit 0
    fi
    echo "trying to connect to minio"
    sleep 1
    counter=$((counter + 1))
  done
}

main() {
  local query_dir
  query_dir=$(check_arg "$@")
  if [ ! -f ./minio ]; then
    download_minio
  fi
  start_minio
  setup_minio "$1"
  upload_data "${query_dir}" "${2:-/repo/tests/}"
  setup_aws_credentials
}

main "$@"
