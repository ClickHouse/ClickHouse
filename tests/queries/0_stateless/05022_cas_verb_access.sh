#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Access control for the three lifecycle verbs added in the rev.8 disk-lifecycle round -- SYSTEM CONTENT
# ADDRESSED FORGET / GC STOP / GC START -- mirroring 05019_cas_fsck_access.sh. For each verb:
#  1) A zero-grant user is denied BEFORE the disk is resolved -- naming a disk that does not exist still
#     yields ACCESS_DENIED (the access check runs ahead of getDisk), not UNKNOWN_DISK.
#  2) Granting the matching right permits the verb; it then fails later on the disk-type check (the always
#     -present `default` disk exists and is never content-addressed, so it deterministically fails with
#     BAD_ARGUMENTS without any lifecycle side effect).
#  3) The verb requires an explicit disk (all three route through the target-required parser like FSCK, so
#     omitting the disk is a client-side SYNTAX_ERROR, not a silent fan-out).
# A unique user name keeps this parallel-safe (no global fixed-name object), and every verb run targets only
# `no_such_disk`/`default`, so nothing is ever actually decommissioned or reconfigured.

USER="user_test_${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} --multiline -q """
DROP USER IF EXISTS ${USER};
CREATE USER ${USER} IDENTIFIED WITH plaintext_password BY 'pw';
REVOKE ALL ON *.* FROM ${USER};
"""

# (1) Zero grants: each verb is denied before the disk is resolved.
${CLICKHOUSE_CLIENT} --multiline --user "${USER}" --password pw -q """
SYSTEM CAS FORGET 'no_such_disk';   -- { serverError ACCESS_DENIED }
"""
${CLICKHOUSE_CLIENT} --multiline --user "${USER}" --password pw -q """
SYSTEM CAS GC STOP 'no_such_disk';  -- { serverError ACCESS_DENIED }
"""
${CLICKHOUSE_CLIENT} --multiline --user "${USER}" --password pw -q """
SYSTEM CAS GC START 'no_such_disk'; -- { serverError ACCESS_DENIED }
"""

# Grant each verb its matching right.
${CLICKHOUSE_CLIENT} --multiline -q """
GRANT SYSTEM CAS FORGET   ON *.* TO ${USER};
GRANT SYSTEM CAS GC STOP  ON *.* TO ${USER};
GRANT SYSTEM CAS GC START ON *.* TO ${USER};
"""

# (2) Granted: the verb is permitted, then fails later on the disk-type check against `default`.
${CLICKHOUSE_CLIENT} --multiline --user "${USER}" --password pw -q """
SYSTEM CAS FORGET default;   -- { serverError BAD_ARGUMENTS }
"""
${CLICKHOUSE_CLIENT} --multiline --user "${USER}" --password pw -q """
SYSTEM CAS GC STOP default;  -- { serverError BAD_ARGUMENTS }
"""
${CLICKHOUSE_CLIENT} --multiline --user "${USER}" --password pw -q """
SYSTEM CAS GC START default; -- { serverError BAD_ARGUMENTS }
"""

# (3) Each verb requires an explicit disk (syntax error).
${CLICKHOUSE_CLIENT} --multiline -q """
SYSTEM CAS FORGET;   -- { clientError SYNTAX_ERROR }
"""
${CLICKHOUSE_CLIENT} --multiline -q """
SYSTEM CAS GC STOP;  -- { clientError SYNTAX_ERROR }
"""
${CLICKHOUSE_CLIENT} --multiline -q """
SYSTEM CAS GC START; -- { clientError SYNTAX_ERROR }
"""

${CLICKHOUSE_CLIENT} --multiline -q """
DROP USER IF EXISTS ${USER};
"""
