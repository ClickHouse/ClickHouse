#!/usr/bin/env bash
# Tags: no-parallel

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Intent (E1):
#  1) A role granted only "SYSTEM CAS GC RUN" is REFUSED (ACCESS_DENIED)
#     when it runs "SYSTEM CAS GC REBUILD <disk>", but ALLOWED to run the per-round
#     "SYSTEM CAS GC RUN". Granting the new
#     "SYSTEM CAS GC REBUILD" right then permits REBUILD.
#  2) "SYSTEM CAS GC REBUILD" with NO disk is a SYNTAX_ERROR (required disk);
#     naming a non-content-addressed disk yields BAD_ARGUMENTS (not a silent all-disks fan-out).
#  3) A user with ZERO grants gets ACCESS_DENIED on the plain
#     "SYSTEM CAS GC RUN 'no_such_disk'" -- the privilege check runs
#     before disk resolution, so denial fires even though the named disk does not exist (it would
#     otherwise be UNKNOWN_DISK).
# (No CA disk needs to exist: the privilege check and the grammar/required-disk check both fire
#  before any disk I/O; assert on the specific error codes. The `default` disk always exists and is
#  never content-addressed, so it deterministically yields BAD_ARGUMENTS once a check is passed.)

${CLICKHOUSE_CLIENT} --multiline -q """
DROP USER IF EXISTS user_test_05011;
CREATE USER user_test_05011 IDENTIFIED WITH plaintext_password BY 'user_test_05011';
REVOKE ALL ON *.* FROM user_test_05011;
GRANT SYSTEM CAS GC RUN ON *.* TO user_test_05011;
"""

# GC-only role: REBUILD is refused; the per-round GC is allowed (fails later, on the disk-type check).
${CLICKHOUSE_CLIENT} --multiline --user user_test_05011 --password user_test_05011 -q """
SYSTEM CAS GC REBUILD default; -- { serverError ACCESS_DENIED }
SYSTEM CAS GC RUN default; -- { serverError BAD_ARGUMENTS }
"""

${CLICKHOUSE_CLIENT} --multiline -q """
GRANT SYSTEM CAS GC REBUILD ON *.* TO user_test_05011;
"""

# Granting the new right permits REBUILD (fails later, on the disk-type check).
${CLICKHOUSE_CLIENT} --multiline --user user_test_05011 --password user_test_05011 -q """
SYSTEM CAS GC REBUILD default; -- { serverError BAD_ARGUMENTS }
"""

# REBUILD requires an explicit disk (syntax error), and never silently fans out across all disks.
${CLICKHOUSE_CLIENT} --multiline -q """
SYSTEM CAS GC REBUILD; -- { clientError SYNTAX_ERROR }
SYSTEM CAS GC REBUILD default; -- { serverError BAD_ARGUMENTS }
"""

# A zero-grant user is denied before the disk is even resolved: naming a disk that does not exist
# still yields ACCESS_DENIED, not UNKNOWN_DISK.
${CLICKHOUSE_CLIENT} --multiline -q """
DROP USER IF EXISTS user_test_05011_zero_grants;
CREATE USER user_test_05011_zero_grants IDENTIFIED WITH plaintext_password BY 'user_test_05011_zero_grants';
REVOKE ALL ON *.* FROM user_test_05011_zero_grants;
"""

${CLICKHOUSE_CLIENT} --multiline --user user_test_05011_zero_grants --password user_test_05011_zero_grants -q """
SYSTEM CAS GC RUN 'no_such_disk'; -- { serverError ACCESS_DENIED }
"""

${CLICKHOUSE_CLIENT} --multiline -q """
DROP USER IF EXISTS user_test_05011;
DROP USER IF EXISTS user_test_05011_zero_grants;
"""
