#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the XGBoost contrib, which is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `predictXGBoost` performs the same `dictGet` access check as `dictGet` itself. The check comes before
# everything else that could fail, so a user without the grant learns nothing about the dictionary: not
# whether it exists, not its layout, and not even when there are no rows to predict.

user="${CLICKHOUSE_DATABASE}_xgb_user_$RANDOM$RANDOM"
dict="${CLICKHOUSE_DATABASE}.xgb_acl"
flat_dict="${CLICKHOUSE_DATABASE}.xgb_acl_flat"
missing_dict="${CLICKHOUSE_DATABASE}.xgb_acl_missing"

$CLICKHOUSE_CLIENT --enable_xgboost=1 --multiquery "
DROP USER IF EXISTS ${user};
DROP DICTIONARY IF EXISTS xgb_acl;
DROP DICTIONARY IF EXISTS xgb_acl_flat;
DROP TABLE IF EXISTS xgb_acl_src;
DROP TABLE IF EXISTS xgb_acl_flat_src;

CREATE TABLE xgb_acl_src (x1 Float64, x2 Float64, y Float64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO xgb_acl_src SELECT number AS x1, intDiv(number, 7) AS x2, 2 * x1 + 3 * x2 AS y FROM numbers(100);

CREATE DICTIONARY xgb_acl (x1 Float64, x2 Float64, y Float64)
PRIMARY KEY (x1, x2)
SOURCE(CLICKHOUSE(TABLE 'xgb_acl_src' DB currentDatabase()))
LAYOUT(XGBOOST(num_iterations 10))
LIFETIME(0);

CREATE TABLE xgb_acl_flat_src (id UInt64, value Float64) ENGINE = MergeTree ORDER BY id;
INSERT INTO xgb_acl_flat_src SELECT 1 AS id, 1.0 AS value;

CREATE DICTIONARY xgb_acl_flat (id UInt64, value Float64)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'xgb_acl_flat_src' DB currentDatabase()))
LAYOUT(FLAT())
LIFETIME(0);

CREATE USER ${user};
"

# Prints the error code, and whether the error is the missing `dictGet` grant rather than anything else.
function denied()
{
    $CLICKHOUSE_CLIENT --user "${user}" --enable_xgboost=1 --query "$1" 2>&1 \
        | grep -o -m1 -E "ACCESS_DENIED|BAD_ARGUMENTS|UNKNOWN_[A-Z_]+" | tr '\n' ' '
    $CLICKHOUSE_CLIENT --user "${user}" --enable_xgboost=1 --query "$1" 2>&1 \
        | grep -q "necessary to have the grant dictGet" && echo "dictGet" || echo "another error"
}

# The owner can predict.
$CLICKHOUSE_CLIENT --enable_xgboost=1 --query "SELECT isFinite(predictXGBoost('${dict}', 1.0, 2.0))"

echo "Without the dictGet grant"
denied "SELECT predictXGBoost('${dict}', 1.0, 2.0)"
# No rows to predict: denied all the same.
denied "SELECT predictXGBoost('${dict}', number, 2.0) FROM numbers(0)"
# A dictionary with another layout: denied, not reported as having the wrong layout.
denied "SELECT predictXGBoost('${flat_dict}', 1.0)"
# A dictionary that does not exist: denied, not reported as missing.
denied "SELECT predictXGBoost('${missing_dict}', 1.0, 2.0)"

echo "With the dictGet grant"
$CLICKHOUSE_CLIENT --query "GRANT dictGet ON ${dict} TO ${user}"
$CLICKHOUSE_CLIENT --user "${user}" --enable_xgboost=1 --query "SELECT isFinite(predictXGBoost('${dict}', 1.0, 2.0))"
$CLICKHOUSE_CLIENT --user "${user}" --enable_xgboost=1 --query "SELECT count() FROM (SELECT predictXGBoost('${dict}', number, 2.0) FROM numbers(0))"

$CLICKHOUSE_CLIENT --multiquery "
DROP USER ${user};
DROP DICTIONARY xgb_acl;
DROP DICTIONARY xgb_acl_flat;
DROP TABLE xgb_acl_src;
DROP TABLE xgb_acl_flat_src;
"
