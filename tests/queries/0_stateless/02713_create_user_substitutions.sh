#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-openssl-fips

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS user1_02713, user2_02713, user3_02713, user4_02713, user5_02713, user6_02713, user7_02713, user14_02713, user15_02713, user16_02713";

$CLICKHOUSE_CLIENT --param_password=qwerty1 -q "CREATE USER user1_02713 IDENTIFIED BY {password:String}";
$CLICKHOUSE_CLIENT --param_password=qwerty2 -q "CREATE USER user2_02713 IDENTIFIED WITH PLAINTEXT_PASSWORD BY {password:String}";
$CLICKHOUSE_CLIENT --param_password=qwerty3 -q "CREATE USER user3_02713 IDENTIFIED WITH SHA256_PASSWORD BY {password:String}";
$CLICKHOUSE_CLIENT --param_password=qwerty4 -q "CREATE USER user4_02713 IDENTIFIED WITH DOUBLE_SHA1_PASSWORD BY {password:String}";
$CLICKHOUSE_CLIENT --param_password=qwerty5 -q "CREATE USER user5_02713 IDENTIFIED WITH BCRYPT_PASSWORD BY {password:String}";

# Generated online
$CLICKHOUSE_CLIENT --param_hash=310cef2caff72c0224f38ca8e2141ca6012cd4da550c692573c25a917d9a75e6 \
    -q "CREATE USER user6_02713 IDENTIFIED WITH SHA256_HASH BY {hash:String}";
# Generated with ClickHouse
$CLICKHOUSE_CLIENT --param_hash=5886A74C452575627522F3A80D8B9E239FD8955F \
    -q "CREATE USER user7_02713 IDENTIFIED WITH DOUBLE_SHA1_HASH BY {hash:String}";
# Generated online
$CLICKHOUSE_CLIENT --param_hash=\$2a\$12\$wuohz0HFSBBNE8huN0Yx6.kmWrefiYVKeMp4gsuNoO1rOWwF2FXXC \
    -q "CREATE USER user8_02713 IDENTIFIED WITH BCRYPT_HASH BY {hash:String}";

$CLICKHOUSE_CLIENT --param_server=qwerty9 -q "CREATE USER user9_02713 IDENTIFIED WITH LDAP SERVER {server:String}";
$CLICKHOUSE_CLIENT --param_realm=qwerty10 -q "CREATE USER user10_02713 IDENTIFIED WITH KERBEROS REALM {realm:String}";
$CLICKHOUSE_CLIENT --param_cert1=qwerty11 --param_cert2=qwerty12 -q "CREATE USER user11_02713 IDENTIFIED WITH SSL_CERTIFICATE CN {cert1:String}, {cert2:String}";
$CLICKHOUSE_CLIENT --param_server=basic_server -q "CREATE USER user12_02713 IDENTIFIED WITH http SERVER {server:String}"
$CLICKHOUSE_CLIENT --param_server=basic_server -q "CREATE USER user13_02713 IDENTIFIED WITH http SERVER {server:String} SCHEME 'Basic'"

$CLICKHOUSE_CLIENT --param_password=qwerty14 -q "CREATE USER user14_02713 IDENTIFIED WITH SCRAM_SHA256_PASSWORD BY {password:String}";

# The `param_hash` is the SCRAM SHA256 digest to create new user `user15_02713` with password `qwerty15` used later below.
$CLICKHOUSE_CLIENT --param_hash=730f506ba74834a27799ded2cc4d94fdfeb43d27244059662ce45a59279976ae \
    -q "CREATE USER user15_02713 IDENTIFIED WITH SCRAM_SHA256_HASH BY {hash:String}";

$CLICKHOUSE_CLIENT --param_password=qwerty16 --param_user=user16_02713 -q "CREATE USER {user:Identifier} IDENTIFIED WITH PLAINTEXT_PASSWORD BY {password:String}";

$CLICKHOUSE_CLIENT --user=user1_02713 --password=qwerty1 -q "SELECT 1";
$CLICKHOUSE_CLIENT --user=user2_02713 --password=qwerty2 -q "SELECT 2";
$CLICKHOUSE_CLIENT --user=user3_02713 --password=qwerty3 -q "SELECT 3";
$CLICKHOUSE_CLIENT --user=user4_02713 --password=qwerty4 -q "SELECT 4";
$CLICKHOUSE_CLIENT --user=user5_02713 --password=qwerty5 -q "SELECT 5";
$CLICKHOUSE_CLIENT --user=user6_02713 --password=qwerty6 -q "SELECT 6";
$CLICKHOUSE_CLIENT --user=user7_02713 --password=qwerty7 -q "SELECT 7";
$CLICKHOUSE_CLIENT --user=user8_02713 --password=qwerty8 -q "SELECT 8";
$CLICKHOUSE_CLIENT --user=user14_02713 --password=qwerty14 -q "SELECT 14";
$CLICKHOUSE_CLIENT --user=user15_02713 --password=qwerty15 -q "SELECT 15";
$CLICKHOUSE_CLIENT --user=user16_02713 --password=qwerty16 -q "SELECT 16";

$CLICKHOUSE_CLIENT -q "SHOW CREATE USER user9_02713";
$CLICKHOUSE_CLIENT -q "SHOW CREATE USER user10_02713";
$CLICKHOUSE_CLIENT -q "SHOW CREATE USER user11_02713";
$CLICKHOUSE_CLIENT -q "SHOW CREATE USER user12_02713";
$CLICKHOUSE_CLIENT -q "SHOW CREATE USER user13_02713";
$CLICKHOUSE_CLIENT -q "SHOW CREATE USER user16_02713";

$CLICKHOUSE_CLIENT -q "DROP USER user1_02713, user2_02713, user3_02713, user4_02713, user5_02713, user6_02713, user7_02713, user8_02713, user9_02713, user10_02713, user11_02713, user12_02713, user13_02713, user14_02713, user15_02713, user16_02713";
