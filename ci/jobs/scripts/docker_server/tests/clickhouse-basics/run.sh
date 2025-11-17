#!/bin/bash
set -eo pipefail

dir="$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")"
lib_dir="$(realpath "${dir}/../../../../../tmp/docker-library/official-images/test")"

image="$1"

CLICKHOUSE_TEST_SLEEP=2
CLICKHOUSE_TEST_TRIES=5

export CLICKHOUSE_USER='my_cool_ch_user'
export CLICKHOUSE_PASSWORD='my cool clickhouse password'

cname="clickhouse-container-$RANDOM-$RANDOM"
cid="$(
  docker run -d \
    -e CLICKHOUSE_USER \
    -e CLICKHOUSE_PASSWORD \
    --name "$cname" \
    "$image"
)"
trap 'docker rm -vf $cid > /dev/null' EXIT

chCli() {
  docker run --rm -i \
    --link "$cname":clickhouse \
    -e CLICKHOUSE_USER \
    -e CLICKHOUSE_PASSWORD \
    "$image" \
    clickhouse-client \
    --host clickhouse \
    --user "$CLICKHOUSE_USER" \
    --password "$CLICKHOUSE_PASSWORD" \
    --query "$*"
}

. "$lib_dir/retry.sh" \
  --tries "$CLICKHOUSE_TEST_TRIES" \
  --sleep "$CLICKHOUSE_TEST_SLEEP" \
  chCli SELECT 1

chCli SHOW DATABASES | grep '^system$' >/dev/null
