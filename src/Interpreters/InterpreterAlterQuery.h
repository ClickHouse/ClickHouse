#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/IStorage_fwd.h>


namespace DB
{

class AccessRightsElements;
class ASTAlterCommand;
class ASTAlterQuery;


/** Allows you add or remove a column in the table.
  * It also allows you to manipulate the partitions of the MergeTree family tables.
  */
class InterpreterAlterQuery : public IInterpreter, WithMutableContext
{
public:
    enum class RowExistsColumnKind
    {
        Regular,
        LightweightDeleteMarker,
        Unknown,
    };

    InterpreterAlterQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_);

    BlockIO execute() override;

    /// `row_exists_column_kind` distinguishes the hidden lightweight-delete marker from an ordinary
    /// physical column. An unknown kind requires both `ALTER DELETE` and `ALTER UPDATE` for `_row_exists = 0`.
    static AccessRightsElements getRequiredAccessForCommand(
        const ASTAlterCommand & command, const String & database, const String & table, RowExistsColumnKind row_exists_column_kind);

    /// Returns unknown when the target storage is not available on the submitting host.
    static RowExistsColumnKind getRowExistsColumnKind(const StoragePtr & storage, const ContextPtr & context_);

    void extendQueryLogElemImpl(QueryLogElement & elem, const ASTPtr & ast, ContextPtr context) const override;

    bool supportsTransactions() const override { return true; }

    /// Skip this query's own access check. Set only for the internal `ATTACH PARTITION` that fills the
    /// temporary table of a `CREATE OR REPLACE TABLE ... CLONE AS` (see `fillTableIfNeeded`): the query
    /// addresses a random `_tmp_replace_*` name that no grant can cover, and the caller has already
    /// authorized the very same access -- `getRequiredAccessForCommand` -- against the user-visible name
    /// the table is published under. Never set this for a user-visible target table.
    void setSkipAccessCheck(bool skip) { skip_access_check = skip; }

private:
    AccessRightsElements getRequiredAccess(const StoragePtr & storage) const;

    BlockIO executeToTable(const ASTAlterQuery & alter);

    BlockIO executeToDatabase(const ASTAlterQuery & alter);

    ASTPtr query_ptr;
    bool skip_access_check = false;
};

}
