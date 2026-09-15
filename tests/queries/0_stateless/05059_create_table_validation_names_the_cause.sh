#!/usr/bin/env bash
# Tags: no-old-analyzer

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

run()
{
    echo "--- $1"
    ${CLICKHOUSE_CLIENT} -q "$1" 2>&1 | grep -m1 -E '^Code: ' \
        | sed -e 's/^Code: \([0-9]*\)\. DB::Exception: Received from [^ ]* DB::Exception: /Code: \1. /' \
              -e 's/^Code: \([0-9]*\)\. DB::Exception: /Code: \1. /' \
              -e 's/_tmp_alter[0-9]*/_tmp_alter<random>/g' \
              -e 's/ (version [^)]*)$//'
}

echo '=== a DEFAULT expression that does not resolve names the identifier the user wrote'
# The validation used to build `_CAST(b_tmp_alter<random>, 'UInt32') AS b, nosuch AS b_tmp_alter<random>`
# and report the synthetic alias - a name that appears nowhere in the query - as the unknown identifier,
# offering it as the hint for itself.
run "CREATE TABLE t (a UInt32, b UInt32 DEFAULT nosuch) ENGINE = MergeTree ORDER BY a"
run "CREATE TABLE t (a UInt32, b UInt32 MATERIALIZED nosuch) ENGINE = MergeTree ORDER BY a"
run "CREATE TABLE t (a UInt32, b UInt32 ALIAS nosuch) ENGINE = MergeTree ORDER BY a"
# A typo of a real column now gets the hint that names it.
run "CREATE TABLE t (aaaa UInt32, b UInt32 DEFAULT aaab) ENGINE = MergeTree ORDER BY aaaa"
# A column with no explicit type took a different path and was already reported correctly.
run "CREATE TABLE t (a UInt32, b DEFAULT nosuch) ENGINE = MergeTree ORDER BY a"
# An expression that resolves but has an incompatible type is still reported as such.
run "CREATE TABLE t (a UInt32, b UInt32 DEFAULT 'not a number') ENGINE = MergeTree ORDER BY a"

echo
echo '=== the same for ALTER, which builds the pair of expressions in its own place'
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS at; CREATE TABLE at (a UInt32, s String) ORDER BY a"
run "ALTER TABLE at ADD COLUMN z UInt32 DEFAULT nosuch"
run "ALTER TABLE at MODIFY COLUMN s DEFAULT nosuch"
# Changing the type of a column that has a default re-checks the existing default expression.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS at2; CREATE TABLE at2 (a UInt32, b UInt32 DEFAULT a * 2) ORDER BY a"
run "ALTER TABLE at2 MODIFY COLUMN b Date"

echo
echo '=== a key that cannot be used names the element that cannot be used'
run "CREATE TABLE t (a UInt32, b Nullable(String), c UInt32) ENGINE = MergeTree ORDER BY (a, b, c)"
run "CREATE TABLE t (a UInt32, b Nullable(UInt32)) ENGINE = MergeTree PARTITION BY b ORDER BY a"
run "CREATE TABLE t (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY (a, 1, b)"

# A DEFAULT expression and a nullable key that are both fine still work.
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE ok (a UInt32, b UInt32 DEFAULT a * 2, c Nullable(UInt32))
    ENGINE = MergeTree ORDER BY (a, c) SETTINGS allow_nullable_key = 1;
    INSERT INTO ok (a, c) VALUES (1, NULL);
    SELECT a, b, c IS NULL FROM ok;
"

${CLICKHOUSE_CLIENT} -q "DROP TABLE at; DROP TABLE at2; DROP TABLE ok"
