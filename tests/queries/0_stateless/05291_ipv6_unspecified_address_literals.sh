#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Every spelling of the IPv6 unspecified address is an address, in HOST IP and in X-Forwarded-For.

for spelling in '::' '0:0:0:0:0:0:0:0' '0::0' '::0.0.0.0'; do
    ${CLICKHOUSE_CLIENT} --query "SELECT formatQuery('CREATE USER u HOST IP ''${spelling}''') FORMAT TSVRaw"
done

${CLICKHOUSE_CLIENT} --query "SELECT formatQuery('CREATE USER u HOST IP '':::''') FORMAT TSVRaw" 2>&1 | grep -o -m1 'Invalid address'

test_user="ipv6_unspecified_user_${CLICKHOUSE_DATABASE}"
test_quota="ipv6_unspecified_quota_${CLICKHOUSE_DATABASE}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "
        DROP QUOTA IF EXISTS ${test_quota};
        DROP USER IF EXISTS ${test_user};
    " >/dev/null 2>&1 || true
}

trap cleanup EXIT

${CLICKHOUSE_CLIENT} --query "
    DROP QUOTA IF EXISTS ${test_quota};
    DROP USER IF EXISTS ${test_user};

    CREATE USER ${test_user};
    GRANT SELECT ON system.quota_usage TO ${test_user};
    CREATE QUOTA ${test_quota} KEYED BY forwarded_ip_address FOR INTERVAL 1 YEAR TRACKING ONLY TO ${test_user};
"

for value in '::' '0:0:0:0:0:0:0:0' '0::0' '::0.0.0.0' '::1' ':::' '::%no-such-interface' 'not-an-ip'; do
    quota_key=$(${CLICKHOUSE_CURL} -sS -H "X-Forwarded-For: ${value}" "${CLICKHOUSE_URL}&user=${test_user}" \
        -d "SELECT quota_key FROM system.quota_usage WHERE quota_name = '${test_quota}'")
    printf '%s\t%s\n' "${value}" "${quota_key}"
done
