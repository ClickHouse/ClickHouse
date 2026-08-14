#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The classifier functions perform the same dictGet access check as dictGet itself, so a user without the
# dictGet grant on the model must be denied, and granting it must let them through.

user="${CLICKHOUSE_DATABASE}_nb_user_$RANDOM$RANDOM"
dict="${CLICKHOUSE_DATABASE}.nb_acl"

$CLICKHOUSE_CLIENT --multiquery "
DROP USER IF EXISTS ${user};
DROP DICTIONARY IF EXISTS nb_acl;
DROP TABLE IF EXISTS nb_acl_src;

CREATE TABLE nb_acl_src (class_id UInt32, ngram String, count UInt64) ENGINE = MergeTree ORDER BY (class_id, ngram);
INSERT INTO nb_acl_src VALUES (0, 'good', 10), (0, 'great', 8), (1, 'bad', 10), (1, 'awful', 6);

CREATE DICTIONARY nb_acl
(
    ngram String,
    class_id UInt32 DEFAULT 0,
    count UInt64 DEFAULT 0
)
PRIMARY KEY ngram
SOURCE(CLICKHOUSE(TABLE 'nb_acl_src' DB currentDatabase()))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token'))
LIFETIME(0);

CREATE USER ${user};
"

# The owner can classify.
$CLICKHOUSE_CLIENT --query "SELECT naiveBayesClassifier('${dict}', 'good great')"

# Without the dictGet grant, every classifier function (and dictGet) is denied.
$CLICKHOUSE_CLIENT --user "${user}" --query "SELECT naiveBayesClassifier('${dict}', 'good great')" 2>&1 | grep -o -m1 'ACCESS_DENIED'
$CLICKHOUSE_CLIENT --user "${user}" --query "SELECT naiveBayesClassifierWithProb('${dict}', 'good great')" 2>&1 | grep -o -m1 'ACCESS_DENIED'
$CLICKHOUSE_CLIENT --user "${user}" --query "SELECT naiveBayesClassifierWithAllProbs('${dict}', 'good great')" 2>&1 | grep -o -m1 'ACCESS_DENIED'
$CLICKHOUSE_CLIENT --user "${user}" --query "SELECT dictGet('${dict}', 'class_id', 'good great')" 2>&1 | grep -o -m1 'ACCESS_DENIED'

# After granting dictGet on the model, the user can classify.
$CLICKHOUSE_CLIENT --query "GRANT dictGet ON ${dict} TO ${user}"
$CLICKHOUSE_CLIENT --user "${user}" --query "SELECT naiveBayesClassifier('${dict}', 'good great')"
$CLICKHOUSE_CLIENT --user "${user}" --query "SELECT (naiveBayesClassifierWithProb('${dict}', 'good great')).1"

$CLICKHOUSE_CLIENT --multiquery "
DROP USER ${user};
DROP DICTIONARY nb_acl;
DROP TABLE nb_acl_src;
"
