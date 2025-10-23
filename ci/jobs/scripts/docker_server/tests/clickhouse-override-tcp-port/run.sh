#!/bin/bash
set -eo pipefail

dir="$(dirname "$(readlink -f "$BASH_SOURCE")")"

image="$1"

CLICKHOUSE_TEST_SLEEP=3
CLICKHOUSE_TEST_TRIES=5

serverImage="$("$dir/../image-name.sh" librarytest/clickhouse-override-tcp-port "$image")"
"$dir/../docker-build.sh" "$dir" "$serverImage" <<EOD
FROM $image
COPY dir/initdb.sql /docker-entrypoint-initdb.d/
COPY dir/override.xml /etc/clickhouse-server/config.d/
EOD

cname="clickhouse-container-$RANDOM-$RANDOM"
cid="$(
	docker run -d \
		--name "$cname" \
		"$serverImage"
)"
trap "docker rm -vf $cid > /dev/null" EXIT

chCli() {
	docker run --rm -i \
		--link "$cname":clickhouse \
		--entrypoint sh \
		"$image" \
		clickhohuse-client \
		--host clickhouse \
		--port 19000 \
		--query "$@"
}

. "$dir/../../retry.sh" --tries "$CLICKHOUSE_TEST_TRIES" --sleep "$CLICKHOUSE_TEST_SLEEP" chCli SELECT 1

chCli "SHOW TABLES FROM test_db" | grep '^test_table$' >/dev/null
[ "$(chCli 'SELECT SUM(value) FROM test_db.test_table')" = 200 ]
