#include <Interpreters/QueryKindUnderReadonly.h>


namespace DB
{

/// The switch is exhaustive on purpose - adding a new `QueryKind` forces an explicit decision here.
bool isQueryKindRejectedUnderReadonly(IAST::QueryKind kind)
{
    switch (kind)
    {
        /// Read-only: allowed to run under `readonly`.
        case IAST::QueryKind::None: /// Unclassified queries have no known write; treat them as read-only.
        case IAST::QueryKind::Select:
        case IAST::QueryKind::Show:
        case IAST::QueryKind::Exists:
        case IAST::QueryKind::Describe:
        case IAST::QueryKind::Explain:
        case IAST::QueryKind::Check:
        /// Session- and transaction-mutating statements (`USE`, `SET` / `SET ROLE`, `BEGIN` / `COMMIT` / `ROLLBACK` /
        /// `SET TRANSACTION SNAPSHOT`) run under `readonly = 2`, so they are not rejected. Their session-visible
        /// side effects are reported separately by `queryKindHasSideEffectsUnderReadonly`.
        case IAST::QueryKind::Use:
        case IAST::QueryKind::Set:
        case IAST::QueryKind::Begin:
        case IAST::QueryKind::Commit:
        case IAST::QueryKind::Rollback:
        case IAST::QueryKind::SetTransactionSnapshot:
        /// BACKUP and RESTORE run under `readonly = 2`, because `BackupsWorker` rejects them only under the strict,
        /// user-set `readonly = 1`. So they are not rejected here; their durable side effects are reported
        /// separately by `queryKindHasSideEffectsUnderReadonly`.
        case IAST::QueryKind::Backup:
        case IAST::QueryKind::Restore:
            return false;

        /// Mutating: rejected under `readonly`.
        case IAST::QueryKind::Insert:
        case IAST::QueryKind::Delete:
        case IAST::QueryKind::Update:
        case IAST::QueryKind::Create:
        case IAST::QueryKind::Drop:
        case IAST::QueryKind::Undrop:
        case IAST::QueryKind::Rename:
        case IAST::QueryKind::Optimize:
        case IAST::QueryKind::Alter:
        case IAST::QueryKind::Grant:
        case IAST::QueryKind::Revoke:
        case IAST::QueryKind::Move:
        case IAST::QueryKind::System:
        case IAST::QueryKind::KillQuery:
        case IAST::QueryKind::ExternalDDL:
        case IAST::QueryKind::AsyncInsertFlush:
        /// `statement1 PARALLEL WITH statement2 ...` says nothing about what runs - the wrapped statements do, each
        /// under a copy of the context, keeping `readonly`. The entry is conservatively mutating; a caller that
        /// cares has to look through the wrapper and classify the statements themselves.
        case IAST::QueryKind::ParallelWithQuery:
        case IAST::QueryKind::Copy:
        case IAST::QueryKind::Snapshot:
            return true;
    }
}

bool queryKindHasSideEffectsUnderReadonly(IAST::QueryKind kind)
{
    switch (kind)
    {
        case IAST::QueryKind::Backup:
        case IAST::QueryKind::Restore:
        case IAST::QueryKind::Set:
        case IAST::QueryKind::Use:
        case IAST::QueryKind::Begin:
        case IAST::QueryKind::Commit:
        case IAST::QueryKind::Rollback:
        case IAST::QueryKind::SetTransactionSnapshot:
            return true;
        default:
            return false;
    }
}

}
