#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/103324.
#
# Before the fix, `InterpreterSetQuery::applySettingsFromQuery` called
# `BackupSettings::fromBackupQuery` for `BACKUP` queries (and the analogous
# path for `RESTORE`), which in turn called
# `BackupInfo::fromAST(*query.base_backup_name)`. When the query parameter
# inside `base_backup = S3({backup_name:String}, ...)` had not yet been
# substituted -- the call runs on the client side BEFORE
# `ReplaceQueryParameterVisitor` -- `BackupInfo::fromAST` raised
# `BAD_ARGUMENTS` "Expected literal, got {backup_name:String}".
#
# The fix uses lightweight `extractCoreSettingsFromQuery` helpers that only
# extract non-backup-specific core settings and never touch
# `base_backup_name`, so parameterized queries pass through the early
# settings-apply step unchanged. Server-side `applySettingsFromQuery` is
# called AFTER substitution, so it has always worked.
#
# Regression introduced by PR #99205 (commit `e6721ef7b16`).

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_103324"
$CLICKHOUSE_CLIENT --query "CREATE TABLE t_103324 (a Int) ENGINE = Memory"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_103324 VALUES (1)"

# `BACKUP` path: before the fix this raised `BAD_ARGUMENTS` synchronously on
# the client, before the asynchronous backup operation was even queued.
# `FORMAT Null` discards the operation UUID; the asynchronous backup itself
# may fail later for unrelated reasons (the destination is intentionally
# unreachable), which is irrelevant to this regression test.
$CLICKHOUSE_CLIENT --param_backup_name='/tmp/x_103324' --query "
BACKUP TABLE t_103324 TO S3('s3://test103324.invalid/x', 'k', 's')
    SETTINGS base_backup = S3({backup_name:String}, 'k', 's')
    ASYNC FORMAT Null
" 2>&1 | { grep -F BAD_ARGUMENTS || echo BACKUP_OK; }

# Symmetric check for the `RESTORE` path, which goes through
# `RestoreSettings::extractCoreSettingsFromQuery`.
$CLICKHOUSE_CLIENT --param_backup_name='/tmp/x_103324' --query "
RESTORE TABLE t_103324 FROM S3('s3://test103324.invalid/x', 'k', 's')
    SETTINGS base_backup = S3({backup_name:String}, 'k', 's')
    ASYNC FORMAT Null
" 2>&1 | { grep -F BAD_ARGUMENTS || echo RESTORE_OK; }

# Sanity check: a non-backup core setting in the same `SETTINGS` clause must
# still be applied on the client side (this is the original PR #99205
# motivation -- `max_execution_time` reaching the context before
# `ProcessList::insert`). We just exercise the code path; the actual setting
# value cannot be observed for `BACKUP ... ASYNC`, but the parse + apply
# step must not throw.
$CLICKHOUSE_CLIENT --param_backup_name='/tmp/x_103324' --query "
BACKUP TABLE t_103324 TO S3('s3://test103324.invalid/y', 'k', 's')
    SETTINGS base_backup = S3({backup_name:String}, 'k', 's'), max_execution_time = 60
    ASYNC FORMAT Null
" 2>&1 | { grep -F BAD_ARGUMENTS || echo CORE_SETTING_OK; }

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_103324"
