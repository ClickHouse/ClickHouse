#!/usr/bin/env bash
# Tags: no-parallel

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Access control for `SYSTEM CAS FSCK` (mirrors 05011_cas_gc_rebuild_access.sh):
#  1) A zero-grant user is denied before the disk is even resolved -- naming a disk that does not
#     exist still yields ACCESS_DENIED, not UNKNOWN_DISK.
#  2) Granting "SYSTEM CAS FSCK" permits the verb; it then fails later, on the
#     disk-type check (the `default` disk always exists and is never content-addressed, so the
#     query deterministically fails with BAD_ARGUMENTS instead).
# (The UNMOUNT/MOUNT siblings this file once covered were removed with the Dormant lifecycle,
#  spec rev.8 §9; FORGET / GC STOP / GC START access coverage is tracked for the acceptance task.)

${CLICKHOUSE_CLIENT} --multiline -q """
DROP USER IF EXISTS user_test_05019;
CREATE USER user_test_05019 IDENTIFIED WITH plaintext_password BY 'user_test_05019';
REVOKE ALL ON *.* FROM user_test_05019;
"""

# Zero grants: denied before the disk is resolved.
${CLICKHOUSE_CLIENT} --multiline --user user_test_05019 --password user_test_05019 -q """
SYSTEM CAS FSCK 'no_such_disk'; -- { serverError ACCESS_DENIED }
"""

${CLICKHOUSE_CLIENT} --multiline -q """
GRANT SYSTEM CAS FSCK ON *.* TO user_test_05019;
"""

# Granting the FSCK right permits it (fails later, on the disk-type check).
${CLICKHOUSE_CLIENT} --multiline --user user_test_05019 --password user_test_05019 -q """
SYSTEM CAS FSCK default; -- { serverError BAD_ARGUMENTS }
"""

# The verb requires an explicit disk (syntax error).
${CLICKHOUSE_CLIENT} --multiline -q """
SYSTEM CAS FSCK; -- { clientError SYNTAX_ERROR }
"""

${CLICKHOUSE_CLIENT} --multiline -q """
DROP USER IF EXISTS user_test_05019;
"""
