#!/bin/bash

set -euxf -o pipefail

export RUSTFS_ACCESS_KEY=${RUSTFS_ACCESS_KEY:-clickhouse}
export RUSTFS_SECRET_KEY=${RUSTFS_SECRET_KEY:-clickhouse}
TEST_DIR=${2:-/repo/tests/}

if [ -d "$TEMP_DIR" ]; then
  TEST_DIR=$(readlink -f $TEST_DIR)
  cd "$TEMP_DIR"
  # add / for rustfs and mc in docker
  PATH="/:.:$PATH"
fi

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

download_binaries() {
  local os
  local rustfs_version=${RUSTFS_VERSION:-1.0.0-beta.12}
  local mc_version=${MC_VERSION:-2025-05-21T01-59-54Z}

  os=$(find_os)
  # the musl build is static-pie and runs on any libc
  wget "https://github.com/rustfs/rustfs/releases/download/${rustfs_version}/rustfs-${os}-$(uname -m)-musl-v${rustfs_version}.zip" -O ./rustfs.zip
  unzip -o ./rustfs.zip rustfs
  rm ./rustfs.zip
  wget "https://dl.min.io/client/mc/release/${os}-$(find_arch)/archive/mc.RELEASE.${mc_version}" -O ./mc
  chmod +x ./mc ./rustfs
}

start_rustfs() {
  pwd
  mkdir -p ./rustfs_data
  rustfs --version
  # CI data is throwaway - trade durability (fsync on writes) and background
  # scanner/heal cycles for speed
  export RUSTFS_DURABILITY_MODE=none
  export RUSTFS_SCANNER_SPEED=slowest
  export RUSTFS_HEAL_AUTO_HEAL_ENABLE=false
  export RUSTFS_CONSOLE_ENABLE=false
  nohup rustfs server --address ":11111" ./rustfs_data &
  wait_for_it
  lsof -i :11111
}

setup_rustfs() {
  local test_type=$1
  echo "setup_rustfs(), test_type=$test_type"
  mc alias set clickrustfs http://localhost:11111 "$RUSTFS_ACCESS_KEY" "$RUSTFS_SECRET_KEY"
  mc admin user add clickrustfs test testtest
  mc admin policy attach clickrustfs readwrite --user=test ||:
  mc mb --ignore-existing clickrustfs/test
  if [ "$test_type" = "stateless" ]; then
    echo "Make the test bucket in rustfs anonymously accessible"
    mc anonymous set public clickrustfs/test
  fi
}

# uploads data to rustfs, by default after unpacking all tests
# will be in /usr/share/clickhouse-test/queries
upload_data() {
  local query_dir=$1
  local test_path=$2
  local data_path=${test_path}/queries/${query_dir}/data_minio
  echo "upload_data() data_path=$data_path"

  if [ -d "${data_path}" ]; then
    mc cp --recursive "${data_path}"/ clickrustfs/test/
  fi
}

setup_aws_credentials() {
  mkdir -p ~/.aws
  if [[ -f ~/.aws/credentials ]]; then
    if grep -q "^\[default\]" ~/.aws/credentials; then
        echo "The credentials file contains a [default] section."
        return
    fi
  fi
  cat <<EOT >> ~/.aws/credentials
[default]
aws_access_key_id=${RUSTFS_ACCESS_KEY}
aws_secret_access_key=${RUSTFS_SECRET_KEY}
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
      echo "failed to setup rustfs"
      exit 1
    fi
    echo "trying to connect to rustfs"
    sleep 1
    counter=$((counter + 1))
  done
}

main() {
  local query_dir
  query_dir=$(check_arg "$@")
  if ! (rustfs --version && mc --version); then
    download_binaries
  fi
  setup_aws_credentials
  start_rustfs
  setup_rustfs "$1"
  upload_data "${query_dir}" "$TEST_DIR"
}

main "$@"
