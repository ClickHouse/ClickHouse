#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

exact_profile="${CLICKHOUSE_DATABASE}_division_exact"
legacy_profile="${CLICKHOUSE_DATABASE}_division_legacy"

cleanup()
{
    $CLICKHOUSE_CLIENT --multiquery --query "
        DROP VIEW IF EXISTS division_profile_view;
        DROP SETTINGS PROFILE IF EXISTS ${exact_profile};
        DROP SETTINGS PROFILE IF EXISTS ${legacy_profile};"
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --multiquery --query "
CREATE SETTINGS PROFILE ${exact_profile} SETTINGS sql_compatible_division_for_integers = 1;
CREATE SETTINGS PROFILE ${legacy_profile} SETTINGS sql_compatible_division_for_integers = 0;"

query()
{
    $CLICKHOUSE_CURL -fsS "${CLICKHOUSE_URL}&sql_compatible_division_for_integers=$1" --data-binary "$2"
}

query 0 "SELECT q, toTypeName(q) FROM (SELECT 5 / 2 AS q SETTINGS profile = '${exact_profile}')"
query 1 "SELECT q, toTypeName(q) FROM (SELECT 5 / 2 AS q SETTINGS profile = '${legacy_profile}')"
query 0 "SELECT (SELECT 5 / 2 SETTINGS profile = '${exact_profile}'), 5 / 2"
query 0 "SELECT q FROM (SELECT 5 / 2 AS q SETTINGS sql_compatible_division_for_integers = 0, profile = '${exact_profile}')"
query 0 "SELECT q FROM (SELECT 5 / 2 AS q SETTINGS profile = '${exact_profile}', sql_compatible_division_for_integers = 0)"
query 1 "SELECT q FROM (SELECT 5 / 2 AS q SETTINGS profile = '${exact_profile}', sql_compatible_division_for_integers = DEFAULT)"
query 0 "SELECT q FROM (SELECT q FROM (SELECT 5 / 2 AS q SETTINGS profile = '${exact_profile}') SETTINGS profile = '${legacy_profile}')"
query 0 "CREATE VIEW division_profile_view AS SELECT q FROM (SELECT 5 / 2 AS q SETTINGS profile = '${exact_profile}')"
query 0 "SELECT q FROM division_profile_view"
$CLICKHOUSE_CLIENT --query "ALTER SETTINGS PROFILE ${exact_profile} SETTINGS sql_compatible_division_for_integers = 0"
query 1 "SELECT q FROM division_profile_view"
$CLICKHOUSE_CLIENT --multiquery --query "DETACH TABLE division_profile_view; ATTACH TABLE division_profile_view;"
query 0 "SELECT q FROM division_profile_view"
