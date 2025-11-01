#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "
CREATE USER quoted_by_ip_${CLICKHOUSE_DATABASE};
CREATE USER quoted_by_forwarded_ip_${CLICKHOUSE_DATABASE};

GRANT SELECT, CREATE ON *.* TO quoted_by_ip_${CLICKHOUSE_DATABASE};
GRANT SELECT, CREATE ON *.* TO quoted_by_forwarded_ip_${CLICKHOUSE_DATABASE};

CREATE QUOTA quota_by_ip_${CLICKHOUSE_DATABASE} KEYED BY ip_address IPV4_PREFIX_BITS 24 IPV6_PREFIX_BITS 64 FOR RANDOMIZED INTERVAL 1 YEAR MAX QUERIES = 1 TO quoted_by_ip_${CLICKHOUSE_DATABASE};
"

# Note: the test can be flaky if the randomized interval will end while the loop is run. But with year long interval it's unlikely.
# One query is allowed per quota. Actually two queries will execute successfully due to some implementation specific behaviour.

echo '--- Test with quota with prefix bits ---'

i=0 retries=300
while [[ $i -lt $retries ]]; do
    ((++i))
    ${CLICKHOUSE_CURL} --fail -sS "${CLICKHOUSE_URL}&user=quoted_by_ip_${CLICKHOUSE_DATABASE}" -d "SELECT count() FROM numbers(10)" 2>/dev/null || break
done | uniq

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=quoted_by_ip_${CLICKHOUSE_DATABASE}" -d "SELECT count() FROM numbers(10)" | grep -oF 'exceeded'

# X-Forwarded-For is ignored for quota by immediate IP address
${CLICKHOUSE_CURL} -H 'X-Forwarded-For: 1.2.3.4' -sS "${CLICKHOUSE_URL}&user=quoted_by_ip_${CLICKHOUSE_DATABASE}" -d "SELECT count() FROM numbers(10)" | grep -oF 'exceeded'


echo '--- Test the masked quota key for immediate IP ---'

$CLICKHOUSE_CLIENT --query "
DROP QUOTA IF EXISTS quota_by_ip_${CLICKHOUSE_DATABASE};
CREATE QUOTA quota_by_ip_${CLICKHOUSE_DATABASE}
    KEYED BY ip_address
    IPV4_PREFIX_BITS 24
    IPV6_PREFIX_BITS 64
    FOR RANDOMIZED INTERVAL 1 YEAR MAX QUERIES = 5
    TO quoted_by_ip_${CLICKHOUSE_DATABASE};
"

# Executing a query to create the quota key
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=quoted_by_ip_${CLICKHOUSE_DATABASE}" -d "SELECT 1"

# Check the quota key (works for both IPv4 and IPv6)
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=quoted_by_ip_${CLICKHOUSE_DATABASE}" -d "
SELECT quota_key
FROM system.quota_usage
WHERE quota_name = 'quota_by_ip_${CLICKHOUSE_DATABASE}'
FORMAT TSV;
"

${CLICKHOUSE_CLIENT} --query "
DROP QUOTA IF EXISTS quota_by_ip_${CLICKHOUSE_DATABASE};
"

echo '--- Test the masked quota key for forwarded IP ---'

${CLICKHOUSE_CLIENT} --query "
CREATE QUOTA quota_by_forwarded_ip_${CLICKHOUSE_DATABASE} 
    KEYED BY forwarded_ip_address 
    IPV4_PREFIX_BITS 16 
    FOR RANDOMIZED INTERVAL 1 YEAR MAX QUERIES = 2 
    TO quoted_by_forwarded_ip_${CLICKHOUSE_DATABASE};
"

# Executing a query to create the quota key
${CLICKHOUSE_CURL} -H 'X-Forwarded-For: 1.2.3.4' -sS "${CLICKHOUSE_URL}&user=quoted_by_forwarded_ip_${CLICKHOUSE_DATABASE}" -d "SELECT 1" > /dev/null

${CLICKHOUSE_CURL} -H 'X-Forwarded-For: 1.2.3.4' -sS "${CLICKHOUSE_URL}&user=quoted_by_forwarded_ip_${CLICKHOUSE_DATABASE}" -d "
SELECT quota_key
FROM system.quota_usage
WHERE quota_name = 'quota_by_forwarded_ip_${CLICKHOUSE_DATABASE}'
FORMAT TSV;
" | grep -o "1.2.0.0"

${CLICKHOUSE_CLIENT} --query "
DROP QUOTA IF EXISTS quota_by_ip_${CLICKHOUSE_DATABASE};
DROP QUOTA IF EXISTS quota_by_forwarded_ip_${CLICKHOUSE_DATABASE};

DROP USER IF EXISTS quoted_by_ip_${CLICKHOUSE_DATABASE};
DROP USER IF EXISTS quoted_by_forwarded_ip_${CLICKHOUSE_DATABASE};
"
