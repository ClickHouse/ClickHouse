#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `SQL SECURITY DEFINER` view over a `MergeTree` table without `WHERE` and row policies stays transparent
# (inlined into the invoker's plan) only while it projects stored columns. Any other expression, including an
# `ALIAS` column of the table (directly or through `*`), may carry constants such as keys, which `EXPLAIN`
# would print to the invoker, so such a view is sealed and its plan is hidden.

db=${CLICKHOUSE_DATABASE}
definer="definer_${db}_$RANDOM"
reader="reader_${db}_$RANDOM"

${CLICKHOUSE_CLIENT} <<EOSQL
DROP USER IF EXISTS $definer, $reader;
CREATE USER $definer;
CREATE USER $reader;

CREATE TABLE $db.plain (s String) ENGINE = MergeTree ORDER BY s;
CREATE TABLE $db.with_alias (s String, k String ALIAS concat(s, 'ALIAS_SECRET')) ENGINE = MergeTree ORDER BY s;
INSERT INTO $db.plain VALUES ('a');
INSERT INTO $db.with_alias VALUES ('a');
GRANT SELECT ON $db.plain TO $definer;
GRANT SELECT ON $db.with_alias TO $definer;

CREATE VIEW $db.column_view DEFINER = $definer SQL SECURITY DEFINER AS SELECT s FROM $db.plain;
CREATE VIEW $db.renamed_view DEFINER = $definer SQL SECURITY DEFINER AS SELECT s AS t FROM $db.plain;
CREATE VIEW $db.asterisk_view DEFINER = $definer SQL SECURITY DEFINER AS SELECT * FROM $db.plain;
CREATE VIEW $db.expression_view DEFINER = $definer SQL SECURITY DEFINER AS SELECT concat(s, 'EXPRESSION_SECRET') AS k FROM $db.plain;
CREATE VIEW $db.alias_view DEFINER = $definer SQL SECURITY DEFINER AS SELECT k FROM $db.with_alias;
CREATE VIEW $db.alias_asterisk_view DEFINER = $definer SQL SECURITY DEFINER AS SELECT * FROM $db.with_alias;

GRANT SELECT ON $db.column_view TO $reader;
GRANT SELECT ON $db.renamed_view TO $reader;
GRANT SELECT ON $db.asterisk_view TO $reader;
GRANT SELECT ON $db.expression_view TO $reader;
GRANT SELECT ON $db.alias_view TO $reader;
GRANT SELECT ON $db.alias_asterisk_view TO $reader;
EOSQL

# Prints three flags for the reader's EXPLAIN: a secret is shown, the table is read in the reader's plan, the view is sealed.
for flavour in "EXPLAIN PLAN actions = 1" "EXPLAIN PIPELINE graph = 1, header = 1"; do
    echo "-- $flavour"
    for view in column_view renamed_view asterisk_view expression_view alias_view alias_asterisk_view; do
        out=$(${CLICKHOUSE_CLIENT} --user "$reader" --query "$flavour SELECT * FROM $db.$view" 2>&1)
        flags=()
        for pattern in 'SECRET' 'ReadFromMergeTree' 'ReadFromSealedView'; do
            if grep -q "$pattern" <<< "$out"; then flags+=(1); else flags+=(0); fi
        done
        echo "$view: ${flags[*]}"
    done
done

echo "-- the reader still queries every view"
for view in column_view renamed_view asterisk_view expression_view alias_view alias_asterisk_view; do
    echo "$view: $(${CLICKHOUSE_CLIENT} --user "$reader" --query "SELECT * FROM $db.$view")"
done

${CLICKHOUSE_CLIENT} --query "DROP VIEW $db.column_view; DROP VIEW $db.renamed_view; DROP VIEW $db.asterisk_view; DROP VIEW $db.expression_view; DROP VIEW $db.alias_view; DROP VIEW $db.alias_asterisk_view; DROP USER $definer, $reader"
