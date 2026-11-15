#!/usr/bin/env bash
# Regression for #60304: GRANT CURRENT GRANTS ... WITH REPLACE OPTION must authorize
# the implicit REVOKE ALL, else grant option on one object wipes another user's grants.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

db=${CLICKHOUSE_DATABASE}
atk="atk_04410_${db}_$RANDOM"
victim="victim_04410_${db}_$RANDOM"

${CLICKHOUSE_CLIENT} <<EOF
DROP USER IF EXISTS $atk, $victim;
CREATE TABLE ${db}.t1 (a UInt64) ENGINE = Memory;
CREATE TABLE ${db}.secret (id UInt64, pub String, sec String) ENGINE = MergeTree ORDER BY id;
INSERT INTO ${db}.secret VALUES (1, 'public', 'TOP-SECRET');
CREATE USER $atk, $victim;
-- attacker has grant option only on a couple of objects
GRANT SELECT ON ${db}.t1     TO $atk WITH GRANT OPTION;
GRANT SELECT ON ${db}.secret TO $atk WITH GRANT OPTION;
-- victim holds rights the attacker has no authority over, plus a column restriction
GRANT SELECT, INSERT ON ${db}.t1 TO $victim;
GRANT SELECT ON ${db}.secret     TO $victim;
REVOKE SELECT(sec) ON ${db}.secret FROM $victim;
EOF

# LC_ALL=C pins byte-order collation so grant lines sort identically across CI locales.
clean() { sed -E "s/ (TO|FROM) .*//; s/${db}/db/g" | LC_ALL=C sort; }

echo '1. attacker CURRENT GRANTS ... WITH REPLACE OPTION is denied (issue #60304):'
${CLICKHOUSE_CLIENT} --user "$atk" --query "GRANT CURRENT GRANTS ON *.* TO $victim WITH REPLACE OPTION" 2>&1 | grep -o "ACCESS_DENIED" | head -1

echo '2. victim column restriction still holds (cannot read sec):'
${CLICKHOUSE_CLIENT} --user "$victim" --query "SELECT sec FROM ${db}.secret" 2>&1 | grep -o "ACCESS_DENIED" | head -1

echo '3. victim original grants are unchanged:'
${CLICKHOUSE_CLIENT} --query "SHOW GRANTS FOR $victim" | clean

echo '4. control: plain WITH REPLACE OPTION on object without grant option is denied:'
${CLICKHOUSE_CLIENT} --user "$atk" --query "GRANT SELECT ON ${db}.t1 TO $victim WITH REPLACE OPTION" 2>&1 | grep -o "ACCESS_DENIED" | head -1

echo '5. admin CURRENT GRANTS ... WITH REPLACE OPTION still works:'
${CLICKHOUSE_CLIENT} --query "GRANT CURRENT GRANTS(SELECT ON ${db}.t1) TO $victim WITH REPLACE OPTION"
${CLICKHOUSE_CLIENT} --query "SHOW GRANTS FOR $victim" | clean

echo '6. plain CURRENT GRANTS (additive, no replace) still works for the attacker within its rights:'
${CLICKHOUSE_CLIENT} --query "REVOKE ALL ON *.* FROM $victim"
${CLICKHOUSE_CLIENT} --user "$atk" --query "GRANT CURRENT GRANTS(SELECT ON ${db}.secret) TO $victim"
${CLICKHOUSE_CLIENT} --query "SHOW GRANTS FOR $victim" | clean

${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS $atk, $victim"
