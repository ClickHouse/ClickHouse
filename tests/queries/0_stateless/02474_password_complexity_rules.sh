#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS u_2474"


$CLICKHOUSE_CLIENT -q "CREATE USER u_2474 IDENTIFIED WITH plaintext_password BY ''" 2>&1  | grep -qF \
    "DB::Exception: Invalid password. The password should: be at least 12 characters long, contain at least 1 numeric character, contain at least 1 lowercase character, contain at least 1 uppercase character, contain at least 1 special character." && echo 'OK' || echo 'FAIL' ||:

$CLICKHOUSE_CLIENT -q "CREATE USER u_2474 IDENTIFIED WITH plaintext_password BY '000000000000'" 2>&1  | grep -qF \
    "DB::Exception: Invalid password. The password should: contain at least 1 lowercase character, contain at least 1 uppercase character, contain at least 1 special character." && echo 'OK' || echo 'FAIL' ||:

$CLICKHOUSE_CLIENT -q "CREATE USER u_2474 IDENTIFIED WITH plaintext_password BY 'a00000000000'" 2>&1  | grep -qF \
    "DB::Exception: Invalid password. The password should: contain at least 1 uppercase character, contain at least 1 special character." && echo 'OK' || echo 'FAIL' ||:

$CLICKHOUSE_CLIENT -q "CREATE USER u_2474 IDENTIFIED WITH plaintext_password BY 'aA0000000000'" 2>&1  | grep -qF \
    "DB::Exception: Invalid password. The password should: contain at least 1 special character." && echo 'OK' || echo 'FAIL' ||:

$CLICKHOUSE_CLIENT -q "CREATE USER u_2474 IDENTIFIED WITH plaintext_password BY 'aA!000000000'" 2>&1  | grep -qF \
    "DB::Exception:" && echo 'FAIL' || echo 'OK' ||:


$CLICKHOUSE_CLIENT -q "DROP USER u_2474"
