#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_CLIENT -n --query "
DROP USER IF EXISTS quoted_by_ip;
DROP USER IF EXISTS quoted_by_forwarded_ip;

DROP QUOTA IF EXISTS quota_by_ip;
DROP QUOTA IF EXISTS quota_by_forwarded_ip;

CREATE USER quoted_by_ip;
CREATE USER quoted_by_forwarded_ip;

GRANT SELECT, CREATE ON *.* TO quoted_by_ip;
GRANT SELECT, CREATE ON *.* TO quoted_by_forwarded_ip;

CREATE QUOTA quota_by_ip KEYED BY ip_address FOR RANDOMIZED INTERVAL 1 YEAR MAX QUERIES = 1 TO quoted_by_ip;
CREATE QUOTA quota_by_forwarded_ip KEYED BY forwarded_ip_address FOR RANDOMIZED INTERVAL 1 YEAR MAX QUERIES = 1 TO quoted_by_forwarded_ip;
"

# Note: the test can be flaky if the randomized interval will end while the loop is run. But with year long interval it's unlikely.
# One query is allowed per quota. Actually two queries will execute successfully due to some implementation specific behaviour.

echo '--- Test with quota by immediate IP ---'

while true; do
    $CLICKHOUSE_CLIENT --user quoted_by_ip --query "SELECT count() FROM numbers(10)" 2>/dev/null || break
done | uniq

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=quoted_by_ip" -d "SELECT count() FROM numbers(10)" | grep -oF 'exceeded'

# X-Forwarded-For is ignored for quota by immediate IP address
${CLICKHOUSE_CURL} -H 'X-Forwarded-For: 1.2.3.4' -sS "${CLICKHOUSE_URL}&user=quoted_by_ip" -d "SELECT count() FROM numbers(10)" | grep -oF 'exceeded'


echo '--- Test with quota by forwarded IP ---'

while true; do
    $CLICKHOUSE_CLIENT --user quoted_by_forwarded_ip --query "SELECT count() FROM numbers(10)" 2>/dev/null || break
done | uniq

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=quoted_by_forwarded_ip" -d "SELECT count() FROM numbers(10)" | grep -oF 'exceeded'

# X-Forwarded-For is respected for quota by forwarded IP address
while true; do
    ${CLICKHOUSE_CURL} -H 'X-Forwarded-For: 1.2.3.4' -sS "${CLICKHOUSE_URL}&user=quoted_by_forwarded_ip" -d "SELECT count() FROM numbers(10)" | grep -oP '^10$' || break
done | uniq

${CLICKHOUSE_CURL} -H 'X-Forwarded-For: 1.2.3.4' -sS "${CLICKHOUSE_URL}&user=quoted_by_forwarded_ip" -d "SELECT count() FROM numbers(10)" | grep -oF 'exceeded'

# Only the last IP address is trusted
${CLICKHOUSE_CURL} -H 'X-Forwarded-For: 5.6.7.8, 1.2.3.4' -sS "${CLICKHOUSE_URL}&user=quoted_by_forwarded_ip" -d "SELECT count() FROM numbers(10)" | grep -oF 'exceeded'

${CLICKHOUSE_CURL} -H 'X-Forwarded-For: 1.2.3.4, 5.6.7.8' -sS "${CLICKHOUSE_URL}&user=quoted_by_forwarded_ip" -d "SELECT count() FROM numbers(10)"

$CLICKHOUSE_CLIENT -n --query "
DROP QUOTA IF EXISTS quota_by_ip;
DROP QUOTA IF EXISTS quota_by_forwarded_ip;

DROP USER IF EXISTS quoted_by_ip;
DROP USER IF EXISTS quoted_by_forwarded_ip;
"
