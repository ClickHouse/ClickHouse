#!/bin/bash
set -eo pipefail

dir="$(dirname "$(readlink -f "$BASH_SOURCE")")"
lib_dir="$(realpath "${dir}/../../../../../tmp/docker-library/official-images/test")"

image="$1"

CLICKHOUSE_TEST_SLEEP=3
CLICKHOUSE_TEST_TRIES=5

export CLICKHOUSE_USER='my_cool_ch_user'
export CLICKHOUSE_PASSWORD='my cool clickhouse password'

serverImage="$("$lib_dir/tests/image-name.sh" librarytest/clickhouse-override-tcp-port "$image")"
"$lib_dir/tests/docker-build.sh" "$dir" "$serverImage" <<EOD
FROM $image
COPY dir/initdb.sql /docker-entrypoint-initdb.d/
COPY dir/override.xml /etc/clickhouse-server/config.d/
EOD

cname="clickhouse-container-$RANDOM-$RANDOM"
cid="$(
	docker run -d \
    -e CLICKHOUSE_USER \
    -e CLICKHOUSE_PASSWORD \
		--name "$cname" \
		"$serverImage"
)"
trap "docker rm -vf $cid > /dev/null" EXIT

chCli() {
  args="$@"
  docker run --rm -i \
    --link "$cname":clickhouse \
    -e CLICKHOUSE_USER \
    -e CLICKHOUSE_PASSWORD \
    "$image" \
    clickhouse-client \
    --host clickhouse \
		--port 19000 \
    --query "$(echo "${args}")"
}

. "$lib_dir/retry.sh" \
  --tries "$CLICKHOUSE_TEST_TRIES" \
  --sleep "$CLICKHOUSE_TEST_SLEEP" \
  chCli SELECT 1

chCli SHOW TABLES FROM test_db | grep '^test_table$' >/dev/null
[ "$(chCli 'SELECT SUM(value) FROM test_db.test_table')" = 200 ]
