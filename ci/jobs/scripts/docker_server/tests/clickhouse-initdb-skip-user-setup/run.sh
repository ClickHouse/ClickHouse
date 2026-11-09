#!/bin/bash
set -eo pipefail

dir="$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")"
source "$dir/../lib.sh"

image="$1"

cid="$(
  docker run -d \
    -e CLICKHOUSE_SKIP_USER_SETUP=1 \
    -v "$dir/initdb.sql":/docker-entrypoint-initdb.d/initdb.sql:ro \
    --name "$(cname)" \
    "$image"
)"
trap 'docker rm -vf $cid > /dev/null' EXIT

chCli() {
  docker run --rm -i \
    --link "$cid":clickhouse \
    "$image" \
    clickhouse-client \
    --host clickhouse \
    --query "$*"
}

# shellcheck source=../../../../../tmp/docker-library/official-images/test/retry.sh
. "$TESTS_LIB_DIR/retry.sh" \
  --tries "$CLICKHOUSE_TEST_TRIES" \
  --sleep "$CLICKHOUSE_TEST_SLEEP" \
  chCli SELECT 1

chCli SHOW TABLES IN test_db | grep '^test_table$' >/dev/null
[ "$(chCli 'SELECT SUM(value) FROM test_db.test_table')" = 200 ]
