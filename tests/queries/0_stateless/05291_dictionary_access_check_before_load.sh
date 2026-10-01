#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Access rights must be checked before a dictionary is loaded,
# so a query without the required grants does not trigger the load.

username="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} -m --query "
    DROP USER IF EXISTS ${username};
    CREATE TABLE src (id UInt64, value String) ENGINE = Memory;
    INSERT INTO src VALUES (1, 'a');
    CREATE DICTIONARY d (id UInt64, value String)
    PRIMARY KEY id
    SOURCE(CLICKHOUSE(TABLE 'src' DB '${CLICKHOUSE_DATABASE}'))
    LAYOUT(FLAT())
    LIFETIME(0);
    CREATE TABLE nb_src (class_id UInt32, ngram String, count UInt64) ENGINE = Memory;
    INSERT INTO nb_src VALUES (0, 'good', 10), (1, 'bad', 10);
    CREATE DICTIONARY nb (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
    PRIMARY KEY ngram
    SOURCE(CLICKHOUSE(TABLE 'nb_src' DB '${CLICKHOUSE_DATABASE}'))
    LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token'))
    LIFETIME(0);
    CREATE TABLE w (id UInt64, value String) ENGINE = Dictionary(${CLICKHOUSE_DATABASE}.d);
    SYSTEM UNLOAD DICTIONARY d;
    CREATE USER ${username} NOT IDENTIFIED;
    GRANT CREATE TEMPORARY TABLE ON *.* TO ${username};
"

# The queries go over HTTP: starting a client for each of them makes the test too slow.
function query()
{
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary "$1"
}

function status()
{
    query "SELECT status FROM system.dictionaries WHERE database = currentDatabase() AND name = '${1:-d}'"
}

function unload()
{
    query "SYSTEM UNLOAD DICTIONARY d"
    status
}

function as_user()
{
    echo "$1" | sed "s/${CLICKHOUSE_DATABASE}/db/g"
    local output
    output=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${username}" --data-binary "$1" 2>&1)
    if grep -q ACCESS_DENIED <<< "${output}"
    then
        echo ACCESS_DENIED
    else
        echo "${output}"
    fi
}

status

echo "--- no grants"
# Both a qualified name and an unqualified one resolved against the current database.
for dict in "${CLICKHOUSE_DATABASE}.d" "d"
do
    as_user "SELECT * FROM dictionary('${dict}')"
    status
    as_user "DESCRIBE TABLE dictionary('${dict}')"
    status
    as_user "SELECT dictGet('${dict}', 'value', toUInt64(1))"
    status
    as_user "SELECT dictHas('${dict}', toUInt64(1))"
    status
    as_user "SELECT count() FROM numbers(1) GROUP BY dictGet('${dict}', 'value', number) SETTINGS enable_analyzer = 0"
    status
done

echo "--- SELECT on a Dictionary table is not enough without a grant on the dictionary"
query "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.w TO ${username}"
as_user "SELECT * FROM w"
status
# A join may use the dictionary directly as a key-value storage, check it is covered as well.
for settings in "join_algorithm = 'direct'" "join_algorithm = 'hash'" "join_algorithm = 'direct', enable_analyzer = 0"
do
    as_user "SELECT n.number, w.value FROM numbers(2) AS n LEFT JOIN w ON n.number = w.id SETTINGS ${settings}"
    status
done
# Queries that are only analyzed do not read the right table, but the join planning must not load the dictionary either.
query "GRANT CREATE VIEW ON ${CLICKHOUSE_DATABASE}.* TO ${username}"
for analyzer in 1 0
do
    as_user "EXPLAIN SELECT n.number, w.value FROM numbers(2) AS n LEFT JOIN w ON n.number = w.id SETTINGS join_algorithm = 'direct', enable_analyzer = ${analyzer}"
    status
    as_user "CREATE VIEW v_by_user AS SELECT n.number, w.value FROM numbers(2) AS n LEFT JOIN w ON n.number = w.id SETTINGS join_algorithm = 'direct', enable_analyzer = ${analyzer}"
    status
done

echo "--- creating a Dictionary table needs access to the dictionary"
${CLICKHOUSE_CLIENT} -m --query "
    GRANT CREATE TABLE ON ${CLICKHOUSE_DATABASE}.* TO ${username};
    GRANT TABLE ENGINE ON Dictionary TO ${username};
"
as_user "CREATE TABLE w_by_user (id UInt64, value String) ENGINE = Dictionary(${CLICKHOUSE_DATABASE}.d)"
status
# An unqualified name is resolved against the current database, like when the table is read.
as_user "CREATE TABLE w_by_user (id UInt64, value String) ENGINE = Dictionary(d)"
status

echo "--- no grants, naiveBayesClassifier"
status nb
as_user "SELECT naiveBayesClassifier('nb', 'good')"
status nb
as_user "SELECT naiveBayesClassifierWithProb('nb', 'good')"
status nb
as_user "SELECT naiveBayesClassifierWithAllProbs('nb', 'good')"
status nb

echo "--- SELECT is enough for the dictionary table function, but not for dictGet"
query "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.d TO ${username}"
as_user "SELECT dictGet('d', 'value', toUInt64(1))"
status
as_user "DESCRIBE TABLE dictionary('d')"
as_user "SELECT * FROM dictionary('d')"
status
# The same rule applies to a direct join against a Dictionary table, with both analyzers.
for analyzer in 1 0
do
    unload
    as_user "SELECT n.number, w.value FROM numbers(2) AS n LEFT JOIN w ON n.number = w.id ORDER BY n.number SETTINGS join_algorithm = 'direct', enable_analyzer = ${analyzer}"
    status
done

echo "--- dictGet"
${CLICKHOUSE_CLIENT} -m --query "
    REVOKE SELECT ON ${CLICKHOUSE_DATABASE}.d FROM ${username};
    GRANT dictGet ON ${CLICKHOUSE_DATABASE}.d TO ${username};
"
unload
as_user "SELECT dictGet('d', 'value', toUInt64(1))"
status
unload
as_user "SELECT dictHas('${CLICKHOUSE_DATABASE}.d', toUInt64(1))"
status
unload
as_user "SELECT count() FROM numbers(1) GROUP BY dictGet('d', 'value', number) SETTINGS enable_analyzer = 0"
status
unload
as_user "SELECT * FROM dictionary('d')"
status
unload
as_user "SELECT n.number, w.value FROM numbers(2) AS n LEFT JOIN w ON n.number = w.id ORDER BY n.number SETTINGS join_algorithm = 'direct'"
status
unload
as_user "CREATE VIEW v_by_user AS SELECT n.number, w.value FROM numbers(2) AS n LEFT JOIN w ON n.number = w.id SETTINGS join_algorithm = 'direct'"
status
# Creating a Dictionary table only validates the columns against the definition, it does not load the dictionary.
unload
as_user "CREATE TABLE w_by_user (id UInt64, value String) ENGINE = Dictionary(${CLICKHOUSE_DATABASE}.d)"
status
as_user "CREATE TABLE w_unqualified_by_user (id UInt64, value String) ENGINE = Dictionary(d)"
status
# Reading the table resolves the same dictionary.
query "SELECT * FROM w_unqualified_by_user"
status

echo "--- dictGet, naiveBayesClassifier"
query "GRANT dictGet ON ${CLICKHOUSE_DATABASE}.nb TO ${username}"
as_user "SELECT naiveBayesClassifier('nb', 'good')"
status nb

query "DROP USER ${username}"
