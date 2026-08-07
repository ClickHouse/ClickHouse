#!/bin/bash

set -euxf -o pipefail

export SEAWEEDFS_ACCESS_KEY=${SEAWEEDFS_ACCESS_KEY:-clickhouse}
export SEAWEEDFS_SECRET_KEY=${SEAWEEDFS_SECRET_KEY:-clickhouse}
# a region must be configured or the aws cli may stall querying the EC2
# instance metadata endpoint; SeaweedFS accepts any region in the signature
export AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION:-us-east-1}
S3_ENDPOINT="http://localhost:11111"
TEST_DIR=${2:-/repo/tests/}

if [ -d "$TEMP_DIR" ]; then
  TEST_DIR=$(readlink -f $TEST_DIR)
  cd "$TEMP_DIR"
  # add / for weed in docker
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

download_seaweedfs() {
  local seaweedfs_version=${SEAWEEDFS_VERSION:-4.41}

  wget "https://github.com/seaweedfs/seaweedfs/releases/download/${seaweedfs_version}/$(find_os)_$(find_arch).tar.gz" -O ./seaweedfs.tar.gz
  tar -xzf ./seaweedfs.tar.gz weed
  rm ./seaweedfs.tar.gz
  chmod +x ./weed
}

# SeaweedFS has no runtime user administration - identities (including the
# anonymous one that makes the test bucket public) are declared in a static
# config read at startup
write_s3_config() {
  local test_type=$1
  cat > ./seaweedfs_s3.json <<EOT
{
  "identities": [
    {
      "name": "clickhouse",
      "credentials": [{"accessKey": "${SEAWEEDFS_ACCESS_KEY}", "secretKey": "${SEAWEEDFS_SECRET_KEY}"}],
      "actions": ["Admin", "Read", "Write", "List", "Tagging"]
    },
    {
      "name": "test",
      "credentials": [{"accessKey": "test", "secretKey": "testtest"}],
      "actions": ["Read", "Write", "List", "Tagging"]
    }
EOT
  if [ "$test_type" = "stateless" ]; then
    cat >> ./seaweedfs_s3.json <<EOT
    ,{
      "name": "anonymous",
      "actions": ["Read:test", "Write:test", "List:test", "Tagging:test"]
    }
EOT
  fi
  cat >> ./seaweedfs_s3.json <<EOT
  ]
}
EOT
}

start_seaweedfs() {
  pwd
  mkdir -p ./seaweedfs_data
  weed version
  # weed server also runs master/volume/filer services (each also binds a gRPC
  # port at +10000); keep them next to the S3 port, away from the ports used by
  # clickhouse-server, keeper, azurite and redpanda
  nohup weed server -dir=./seaweedfs_data \
    -master.port=11112 -volume.port=11113 -filer.port=11114 \
    -s3 -s3.port=11111 -s3.config=./seaweedfs_s3.json \
    -master.volumeSizeLimitMB=1024 -volume.max=0 &
  wait_for_it
  lsof -i :11111
}

setup_seaweedfs() {
  aws --endpoint-url "$S3_ENDPOINT" s3 mb s3://test
  # the S3 endpoint answers before volume allocation is ready on a cold start,
  # so wait until a real write succeeds
  local counter=0
  local max_counter=60
  until echo ready | aws --endpoint-url "$S3_ENDPOINT" s3 cp - s3://test/.write_probe
  do
    if [[ ${counter} == "${max_counter}" ]]; then
      echo "failed to wait for seaweedfs write readiness"
      exit 1
    fi
    echo "waiting for seaweedfs write readiness"
    sleep 1
    counter=$((counter + 1))
  done
  aws --endpoint-url "$S3_ENDPOINT" s3 rm s3://test/.write_probe
}

# uploads data to seaweedfs, by default after unpacking all tests
# will be in /usr/share/clickhouse-test/queries
upload_data() {
  local query_dir=$1
  local test_path=$2
  local data_path=${test_path}/queries/${query_dir}/data_minio
  echo "upload_data() data_path=$data_path"

  if [ -d "${data_path}" ]; then
    aws --endpoint-url "$S3_ENDPOINT" s3 cp --recursive "${data_path}"/ s3://test/
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
aws_access_key_id=${SEAWEEDFS_ACCESS_KEY}
aws_secret_access_key=${SEAWEEDFS_SECRET_KEY}
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
  # unauthenticated GET / returns a bucket listing for the anonymous identity
  # and AccessDenied when there is none (stateful)
  while ! curl "${params[@]}" "${url}" 2>&1 | grep -E "ListAllMyBucketsResult|AccessDenied"
  do
    if [[ ${counter} == "${max_counter}" ]]; then
      echo "failed to setup seaweedfs"
      exit 1
    fi
    echo "trying to connect to seaweedfs"
    sleep 1
    counter=$((counter + 1))
  done
}

main() {
  local query_dir
  query_dir=$(check_arg "$@")
  if ! command -v aws; then
    echo "ERROR: the aws cli is required"
    exit 1
  fi
  if ! weed version; then
    download_seaweedfs
  fi
  setup_aws_credentials
  write_s3_config "$1"
  start_seaweedfs
  setup_seaweedfs
  upload_data "${query_dir}" "$TEST_DIR"
}

main "$@"
