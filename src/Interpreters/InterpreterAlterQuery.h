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
    /// `context_` resolves the source table of `ATTACH`/`REPLACE PARTITION ... FROM`, so that a session
    /// temporary table is checked under `TEMPORARY_DATABASE` rather than the current database. Pass null
    /// to keep the source as written (e.g. for `ON CLUSTER`, where temporary tables are not visible).
    static AccessRightsElements getRequiredAccessForCommand(
        const ASTAlterCommand & command,
        const String & database,
        const String & table,
        RowExistsColumnKind row_exists_column_kind,
        const ContextPtr & context_);

    /// Returns unknown when the target storage is not available on the submitting host.
    static RowExistsColumnKind getRowExistsColumnKind(const StoragePtr & storage, const ContextPtr & context_);

    /// Adds `SELECT` on the columns of `database`.`table` that `expressions` read. `storage` may be null
    /// (an `ON CLUSTER` query for a table this host does not have), then subcolumns are not mapped to their columns.
    static void addRequiredSelectAccessForExpressions(
        AccessRightsElements & required_access,
        const ASTs & expressions,
        const String & database,
        const String & table,
        const StoragePtr & storage,
        const ContextPtr & context_);

    /// The expressions of an `UPDATE` (`ALTER TABLE ... UPDATE` or `UPDATE ... SET`) that copy table data into the
    /// updated columns: the right-hand sides of the assignments and the predicate. Empty for the lightweight-delete form.
    static ASTs getUpdateExpressionsReadingTable(
        const IAST & assignments, IAST * predicate, RowExistsColumnKind row_exists_column_kind);

    void extendQueryLogElemImpl(QueryLogElement & elem, const ASTPtr & ast, ContextPtr context) const override;

    [[nodiscard]] bool supportsTransactions() const override { return true; }

    /// Skip this query's own access check. Set only for the internal `ATTACH PARTITION` that fills the
    /// temporary table of a `CREATE OR REPLACE TABLE ... CLONE AS` (see `fillTableIfNeeded`): the query
    /// addresses a random `_tmp_replace_*` name that no grant can cover, and the caller has already
    /// authorized the very same access -- `getRequiredAccessForCommand` -- against the user-visible name
    /// the table is published under. Never set this for a user-visible target table.
    void setSkipAccessCheck(bool skip) { skip_access_check = skip; }

private:
    [[nodiscard]] AccessRightsElements getRequiredAccess(const StoragePtr & storage) const;

    /// Adds the grants `processSQLSecurityOption` demands for the view's stored SQL security, so replacing the
    /// body of a `DEFINER` or `NONE` view takes the same authority as declaring that security in the first place.
    void addRequiredAccessForModifyQuerySQLSecurity(AccessRightsElements & required_access, const StoragePtr & storage) const;

    /// Adds `SELECT` on the columns read by the expressions of `command` that are evaluated over the table data:
    /// a column declaration of `ADD COLUMN` / `MODIFY COLUMN` (e.g. `public ALIAS secret`), the assignments and
    /// predicate of `UPDATE` (e.g. `UPDATE public = secret`), and `MODIFY TTL` (e.g. `GROUP BY id SET public = any(secret)`).
    void addRequiredSelectAccessForCommandExpressions(
        AccessRightsElements & required_access,
        const ASTAlterCommand & command,
        const StoragePtr & storage,
        RowExistsColumnKind row_exists_column_kind) const;

    BlockIO executeToTable(const ASTAlterQuery & alter);

    BlockIO executeToDatabase(const ASTAlterQuery & alter);

    ASTPtr query_ptr;
    bool skip_access_check = false;
};

}
