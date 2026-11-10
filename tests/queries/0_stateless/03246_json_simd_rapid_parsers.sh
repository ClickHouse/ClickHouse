#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

# Check parser behavior instead of implementation-dependent names in exception stacks.
# `SimdJSON` rejects excessive nesting; iterative `RapidJSON` accepts it.
# Skip the nested value to avoid constructing deeply nested column types.
queries=$(cat <<'SQL'
SET allow_simdjson = 1;
SELECT '{"a":4}'::JSON;
SELECT '{"a":4ab2}'::JSON; -- { error INCORRECT_DATA }
SELECT isNull(accurateCastOrNull(concat('{"x":', repeat('[', 1024), '0', repeat(']', 1024), '}'), 'JSON(SKIP x)'));

SET allow_simdjson = 0;
SELECT '{"a":4}'::JSON;
SELECT '{"a":4ab2}'::JSON; -- { error INCORRECT_DATA }
SELECT isNull(accurateCastOrNull(concat('{"x":', repeat('[', 1024), '0', repeat(']', 1024), '}'), 'JSON(SKIP x)'));

SET allow_simdjson = 1;
SELECT isNull(accurateCastOrNull(concat('{"x":', repeat('[', 1024), '0', repeat(']', 1024), '}'), 'JSON(SKIP x)'));
SQL
)

# Exercise setting changes within one local context and within one client session.
$CLICKHOUSE_LOCAL --multiquery --query "$queries"
$CLICKHOUSE_CLIENT --multiquery --query "$queries"
