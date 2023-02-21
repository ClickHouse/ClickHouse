#!/usr/bin/env bash
# shellcheck disable=SC2046

# In very old (e.g. 1.1.54385) versions of ClickHouse there was a bug in Poco HTTP library:
# Basic HTTP authentication headers was not parsed if the size of URL is exactly 4077 + something bytes.
# So, the user may get authentication error if valid credentials are passed.
# This is a minor issue because it does not have security implications (at worse the user will be not allowed to access).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# In this test we do the opposite: passing the invalid credentials while server is accepting default user without a password.
# And if the bug exists, they will be ignored (treat as empty credentials) and query succeed.

for i in {3950..4100}; do ${CLICKHOUSE_CURL} --user default:12345 "${CLICKHOUSE_URL}&query=SELECT+1"$(perl -e "print '+'x$i") | grep -v -F 'password' ||:; done

# You can check that the bug exists in old version by running the old server in Docker:
# docker run --network host -it --rm clickhouse/clickhouse-server:1.1.54385
