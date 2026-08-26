#!/usr/bin/env bash
# Tags: no-parallel

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Intent: `SYSTEM CAS DROP POOL MEMBER` checks access BEFORE resolving the disk (same
# pattern as the GC/GC REBUILD verbs covered by 05011_cas_gc_rebuild_access.sh), so this needs no
# CA disk at all:
#  1) A user with ZERO grants is refused with ACCESS_DENIED, even though the named disk does not
#     exist (it would otherwise be UNKNOWN_DISK once past the access check).
#  2) After granting "SYSTEM CAS DROP POOL MEMBER", the same query passes the access
#     check and fails later with UNKNOWN_DISK -- proving that grant, and only that grant, is what
#     unlocks the verb.

${CLICKHOUSE_CLIENT} --multiline -q """
DROP USER IF EXISTS user_test_05016;
CREATE USER user_test_05016 IDENTIFIED WITH plaintext_password BY 'user_test_05016';
REVOKE ALL ON *.* FROM user_test_05016;
"""

${CLICKHOUSE_CLIENT} --multiline --user user_test_05016 --password user_test_05016 -q """
SYSTEM CAS DROP POOL MEMBER 'x' FROM DISK 'y'; -- { serverError ACCESS_DENIED }
"""

${CLICKHOUSE_CLIENT} --multiline -q """
GRANT SYSTEM CAS DROP POOL MEMBER ON *.* TO user_test_05016;
"""

${CLICKHOUSE_CLIENT} --multiline --user user_test_05016 --password user_test_05016 -q """
SYSTEM CAS DROP POOL MEMBER 'x' FROM DISK 'y'; -- { serverError UNKNOWN_DISK }
"""

${CLICKHOUSE_CLIENT} --multiline -q """
DROP USER IF EXISTS user_test_05016;
"""
