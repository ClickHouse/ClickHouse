#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ROOT="${CLICKHOUSE_USER_FILES}/${CLICKHOUSE_DATABASE}_dec38"
rm -rf "${ROOT}"
trap 'rm -rf "${ROOT}"' EXIT

# Iceberg stores a decimal min/max bound rounded to the integral part, so the reader moves the
# bound one integral unit outwards before pruning on it. At scale 38 that unit is `10^38` and the
# bound lives in an `Int128`, which holds `1.7 * 10^38`: the shift leaves the type for every bound
# of magnitude above roughly 0.7014. `high` holds one such value per data file and `low` holds two
# that the shift keeps inside the type, so the two tables differ only in whether the shift saturates.
#
# One value per `INSERT`, so each table has two data files and a bound on one can exclude it.
# Background Iceberg compaction, which the cloud build runs off a member flag rather than a query
# setting, would rewrite these manifests, so it is pinned off per table.
mk() { # mk <table> <value> <value>
    cat <<SQL
CREATE TABLE $1 (d Decimal(38, 38)) ENGINE = IcebergLocal('${ROOT}/$1/', 'Parquet')
SETTINGS allow_experimental_iceberg_compaction = 0;
INSERT INTO $1 VALUES (toDecimal128('$2', 38));
INSERT INTO $1 VALUES (toDecimal128('$3', 38));
SQL
}

# An inline VALUES list still consumes stdin, which blocks until EOF if the caller left it open.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --use_iceberg_metadata_files_cache=0 --query "
$(mk high '0.99' '-0.99')
$(mk low '0.1' '-0.1')
" < /dev/null

# One line per tag, in the order given: how many data files <counter> saw that probe prune.
# Reading a counter back through `log_comment` keeps a probe from counting itself.
report() { # report <counter> <tag>...
    local counter=$1 tags="" t
    shift
    for t in "$@"; do tags="${tags:+${tags}, }'${CLICKHOUSE_DATABASE}_${t}'"; done
    ${CLICKHOUSE_CLIENT} --query "
        SELECT max(ProfileEvents['${counter}'])
        FROM system.query_log
        WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN (${tags})
        GROUP BY log_comment
        ORDER BY indexOf([${tags}], log_comment)
        SETTINGS enable_parallel_replicas = 0"
}

echo '--- A0 a filter on the column returns every row that matches it ---'
# A bound that comes back inverted prunes the data file that holds the row, so these lose rows.
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --query "
    SELECT count() FROM high;
    SELECT count() FROM high WHERE d = toDecimal128('0.99', 38);
    SELECT count() FROM high WHERE d = toDecimal128('-0.99', 38);
    SELECT count() FROM high WHERE d > toDecimal128('0.5', 38);
    SELECT count() FROM high WHERE d < toDecimal128('-0.5', 38);"

echo '--- A1 a probe outside one file min/max-prunes it, and only it ---'
# `low` is the control: its shift stays inside `Int128` whatever the reader does with an overflow.
# Each `high` probe sits outside exactly one of the two shifted bounds, so one file is pruned and
# the other is read - the row counts say the surviving file is the one that could hold a match.
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --query "
    SELECT count() FROM low WHERE d = toDecimal128('0.95', 38) SETTINGS log_comment = '${CLICKHOUSE_DATABASE}_low_pos';
    SELECT count() FROM low WHERE d = toDecimal128('-0.95', 38) SETTINGS log_comment = '${CLICKHOUSE_DATABASE}_low_neg';
    SELECT count() FROM high WHERE d = toDecimal128('0.5', 38) SETTINGS log_comment = '${CLICKHOUSE_DATABASE}_high_pos';
    SELECT count() FROM high WHERE d = toDecimal128('-0.5', 38) SETTINGS log_comment = '${CLICKHOUSE_DATABASE}_high_neg';
    SYSTEM FLUSH LOGS query_log;"
report IcebergMinMaxIndexPrunedFiles low_pos low_neg high_pos high_neg

echo '--- A2 a value inside both shifted bounds prunes nothing ---'
${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --query "
    SELECT count() FROM high WHERE d = toDecimal128('0.0', 38) SETTINGS log_comment = '${CLICKHOUSE_DATABASE}_high_zero';
    SYSTEM FLUSH LOGS query_log;"
report IcebergMinMaxIndexPrunedFiles high_zero

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS high SYNC;
    DROP TABLE IF EXISTS low SYNC;"
