#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# `Poco::Net::IPAddress::toString` used to drop the dotted part of `::0.x.y.z` and `::ffff:0.x.y.z`,
# so the addresses below (`::6` being the one from the issue) were shown as `::`, `::`, `::ffff:`, `::ffff:` and `::1.2.3.4`.
# Users are global, so the name is made unique per test database.
USER="test_user_05385_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${USER}"
$CLICKHOUSE_CLIENT -q "CREATE USER ${USER} HOST IP '::6', '::0.1.2.3', '::ffff:0.0.0.6', '::ffff:0.0.0.0', '::1.2.3.4'"
$CLICKHOUSE_CLIENT -q "SHOW CREATE USER ${USER}" | sed "s/${USER}/test_user/"
$CLICKHOUSE_CLIENT -q "DROP USER ${USER}"
