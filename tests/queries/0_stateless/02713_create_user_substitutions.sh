#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS user1_02713, user2_02713, user3_02713, user4_02713, user5_02713, user6_02713, user7_02713";

$CLICKHOUSE_CLIENT --param_password=qwerty1 -q "CREATE USER user1_02713 IDENTIFIED BY {password:String}";
$CLICKHOUSE_CLIENT --param_password=qwerty2 -q "CREATE USER user2_02713 IDENTIFIED WITH PLAINTEXT_PASSWORD BY {password:String}";
$CLICKHOUSE_CLIENT --param_password=qwerty3 -q "CREATE USER user3_02713 IDENTIFIED WITH SHA256_PASSWORD BY {password:String}";
$CLICKHOUSE_CLIENT --param_password=qwerty4 -q "CREATE USER user4_02713 IDENTIFIED WITH DOUBLE_SHA1_PASSWORD BY {password:String}";
$CLICKHOUSE_CLIENT --param_server=qwerty5 -q "CREATE USER user5_02713 IDENTIFIED WITH LDAP SERVER {server:String}";
$CLICKHOUSE_CLIENT --param_realm=qwerty6 -q "CREATE USER user6_02713 IDENTIFIED WITH KERBEROS REALM {realm:String}";
$CLICKHOUSE_CLIENT --param_cert1=qwerty7 --param_cert2=qwerty8 -q "CREATE USER user7_02713 IDENTIFIED WITH SSL_CERTIFICATE CN {cert1:String}, {cert2:String}";

$CLICKHOUSE_CLIENT --user=user1_02713 --password=qwerty1 -q "SELECT 1";
$CLICKHOUSE_CLIENT --user=user2_02713 --password=qwerty2 -q "SELECT 2";
$CLICKHOUSE_CLIENT --user=user3_02713 --password=qwerty3 -q "SELECT 3";
$CLICKHOUSE_CLIENT --user=user4_02713 --password=qwerty4 -q "SELECT 4";

$CLICKHOUSE_CLIENT -q "SHOW CREATE USER user5_02713";
$CLICKHOUSE_CLIENT -q "SHOW CREATE USER user6_02713";
$CLICKHOUSE_CLIENT -q "SHOW CREATE USER user7_02713";

$CLICKHOUSE_CLIENT -q "DROP USER user1_02713, user2_02713, user3_02713, user4_02713, user5_02713, user6_02713, user7_02713";
