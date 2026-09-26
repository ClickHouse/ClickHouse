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
    InterpreterAlterQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_);

    BlockIO execute() override;

    /// `row_exists_is_lightweight_marker` is true only when `_row_exists` is the hidden virtual
    /// lightweight-delete marker of the target table (not an ordinary physical column with that name);
    /// it gates the `_row_exists = 0` -> ALTER DELETE shortcut in the UPDATE command. Pass false to fail
    /// closed (e.g. a non-local ON CLUSTER target, or ALTER DATABASE).
    static AccessRightsElements getRequiredAccessForCommand(
        const ASTAlterCommand & command, const String & database, const String & table, bool row_exists_is_lightweight_marker);

    /// True when `_row_exists` is the hidden virtual lightweight-delete marker of `storage`, i.e. not
    /// an ordinary physical column with that name (as is possible on engines like `Memory`). Null
    /// storage returns false (fail closed). Used to decide whether `_row_exists = 0` is a delete.
    static bool isRowExistsLightweightDeleteMarker(const StoragePtr & storage, const ContextPtr & context_);

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
