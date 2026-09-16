#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: the encryption functions are not available in the fast test build
# Tag no-replicated-database: SQL SECURITY DEFINER views and users are set up per-test

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Reproduces the reported scenario: a SQL SECURITY DEFINER view derives its HMAC key with decrypt()
# on the opposite side of a JOIN (or in a derived table). A user holding only SELECT on the view,
# without the displaySecretsInShowAndSelect privilege, must not recover the decrypted key through
# any EXPLAIN flavour, while SHOW CREATE keeps hiding the decrypt() arguments and SELECT stays usable.
# The plans are not dumped into the reference (they depend on the configuration); only the presence
# of the plaintext and of the [HIDDEN] placeholder is checked.

db=${CLICKHOUSE_DATABASE}
owner="owner_${db}_$RANDOM"
attacker="attacker_${db}_$RANDOM"
nogrant="nogrant_${db}_$RANDOM"
key='0123456789abcdef'
iv='abcdef9876543210'
plaintext='JOIN_SIDE_SECRET_PLAINTEXT'

ciphertext=$(${CLICKHOUSE_CLIENT} --query "SELECT hex(encrypt('aes-128-cbc', '$plaintext', '$key', '$iv'))")

${CLICKHOUSE_CLIENT} <<EOSQL
DROP USER IF EXISTS $owner, $attacker, $nogrant;
CREATE USER $owner;
CREATE USER $attacker;
CREATE USER $nogrant;
GRANT CREATE VIEW ON $db.* TO $owner;
GRANT CREATE TEMPORARY TABLE ON *.* TO $owner;
EOSQL

${CLICKHOUSE_CLIENT} --user "$owner" <<EOSQL
CREATE VIEW $db.protected_v SQL SECURITY DEFINER AS
SELECT n.number FROM numbers(1) AS n
INNER JOIN (SELECT decrypt('aes-128-cbc', unhex('$ciphertext'), '$key', '$iv') AS k) AS s
    ON HMAC('sha256', toString(n.number), s.k) = ''
SETTINGS enable_analyzer = 1;

CREATE VIEW $db.derived_v SQL SECURITY DEFINER AS
SELECT number FROM (SELECT number, decrypt('aes-128-cbc', unhex('$ciphertext'), '$key', '$iv') AS k FROM numbers(1)) AS s
WHERE empty(HMAC('sha256', toString(number), s.k))
SETTINGS enable_analyzer = 1;

CREATE VIEW $db.pos_literal_v SQL SECURITY DEFINER AS
SELECT number FROM numbers(1) WHERE empty(HMAC('sha256', toString(number), 'DIRECT_LITERAL_SECRET'))
SETTINGS enable_analyzer = 1;

CREATE VIEW $db.pos_alias_v SQL SECURITY DEFINER AS
WITH 'WITH_ALIAS_SECRET' AS k SELECT number FROM numbers(1) WHERE empty(HMAC('sha256', toString(number), k))
SETTINGS enable_analyzer = 1;
EOSQL

for view in protected_v derived_v pos_literal_v pos_alias_v; do
    ${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON $db.$view TO $attacker"
done

as_attacker() { ${CLICKHOUSE_CLIENT} --user "$attacker" --query "$1"; }

echo "-- the attacker has no privilege to display secrets"
as_attacker "CHECK GRANT displaySecretsInShowAndSelect ON *.*"

for view in protected_v derived_v pos_literal_v pos_alias_v; do
    echo "-- $view: SHOW CREATE hides the secret arguments"
    as_attacker "SHOW CREATE VIEW $db.$view FORMAT TSVRaw" | grep -c "HIDDEN"
    as_attacker "SHOW CREATE VIEW $db.$view FORMAT TSVRaw" | grep -c "$plaintext\|_SECRET" || true

    echo "-- $view: SELECT works and returns nothing"
    as_attacker "SELECT count() FROM $db.$view"

    # The pretty format prints constant values, so the key must show up as [HIDDEN] there.
    for options in "actions = 1" "actions = 1, pretty = 1" "header = 1"; do
        echo "-- $view: EXPLAIN PLAN $options: lines with the secret, then whether [HIDDEN] is present"
        as_attacker "EXPLAIN PLAN $options SELECT * FROM $db.$view SETTINGS enable_analyzer = 1" | grep -c "$plaintext\|_SECRET" || true
        as_attacker "EXPLAIN PLAN $options SELECT * FROM $db.$view SETTINGS enable_analyzer = 1" | grep -q "HIDDEN" && echo 1 || echo 0
    done

    # The legacy and JSON formats print node names only; the key is referenced by its column name.
    for options in "actions = 1, pretty = 0, compact = 0" "json = 1, actions = 1"; do
        echo "-- $view: EXPLAIN PLAN $options: lines with the secret"
        as_attacker "EXPLAIN PLAN $options SELECT * FROM $db.$view SETTINGS enable_analyzer = 1" | grep -c "$plaintext\|_SECRET" || true
    done

    echo "-- $view: EXPLAIN QUERY TREE and EXPLAIN SYNTAX: lines with the secret"
    as_attacker "EXPLAIN QUERY TREE SELECT * FROM $db.$view SETTINGS enable_analyzer = 1" | grep -c "$plaintext\|_SECRET" || true
    as_attacker "EXPLAIN SYNTAX SELECT * FROM $db.$view SETTINGS enable_analyzer = 1" | grep -c "$plaintext\|_SECRET" || true
done

echo "-- without the grant the plan is not reachable at all"
${CLICKHOUSE_CLIENT} --user "$nogrant" --query "EXPLAIN PLAN actions = 1 SELECT * FROM $db.protected_v SETTINGS enable_analyzer = 1" 2>&1 | grep -o "ACCESS_DENIED" | head -1

for view in protected_v derived_v pos_literal_v pos_alias_v; do
    ${CLICKHOUSE_CLIENT} --query "DROP VIEW $db.$view"
done
${CLICKHOUSE_CLIENT} --query "DROP USER $owner, $attacker, $nogrant"
