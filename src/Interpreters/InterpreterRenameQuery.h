#pragma once

#include <Interpreters/IInterpreter.h>
#include <Interpreters/StorageID.h>
#include <Parsers/ASTRenameQuery.h>

#include <functional>


namespace DB
{

class AccessRightsElements;
class DDLGuard;

/// To avoid deadlocks, we must acquire locks for tables in same order in any different RENAMEs.
struct UniqueTableName
{
    String database_name;
    String table_name;

    UniqueTableName(const String & database_name_, const String & table_name_)
            : database_name(database_name_), table_name(table_name_) {}

    bool operator< (const UniqueTableName & rhs) const
    {
        return std::tie(database_name, table_name) < std::tie(rhs.database_name, rhs.table_name);
    }
};

struct RenameDescription
{
    RenameDescription(const ASTRenameQuery::Element & elem, const String & current_database) :
            from_database_name(!elem.from.database ? current_database : elem.from.getDatabase()),
            from_table_name(elem.from.getTable()),
            to_database_name(!elem.to.database ? current_database : elem.to.getDatabase()),
            to_table_name(elem.to.getTable()),
            if_exists(elem.if_exists)
    {}

    String from_database_name;
    String from_table_name;

    String to_database_name;
    String to_table_name;
    bool if_exists;
};

using RenameDescriptions = std::vector<RenameDescription>;

using TableGuards = std::map<UniqueTableName, std::unique_ptr<DDLGuard>>;

/** Rename one table
  *  or rename many tables at once.
  */
class InterpreterRenameQuery : public IInterpreter, WithContext
{
public:
    /// Invoked while holding the per-table `DDLGuard`s, immediately before the
    /// non-replicated `database->renameTable` call. Receives the `to` `StorageID`
    /// (the name whose contents are about to be exchanged-with or replaced). The
    /// callback can throw to abort the rename atomically: the guards release via
    /// RAII, no rename happens, and the exception propagates to the caller.
    /// Used by `CREATE OR REPLACE TABLE` to enforce `max_table_size_to_drop` on
    /// the storage as it is at exchange time, closing the `TOCTOU` window
    /// between the upfront pre-flight check and the rename's guard acquisition.
    using PreSwapCheck = std::function<void(const StorageID &)>;

    InterpreterRenameQuery(const ASTPtr & query_ptr_, ContextPtr context_);
    BlockIO execute() override;

    void setPreSwapCheck(PreSwapCheck check) { pre_swap_check = std::move(check); }

    void extendQueryLogElemImpl(QueryLogElement & elem, const ASTPtr & ast, ContextPtr) const override;

    bool renamedInsteadOfExchange() const { return renamed_instead_of_exchange; }

private:
    BlockIO executeToTables(const ASTRenameQuery & rename, const RenameDescriptions & descriptions, TableGuards & ddl_guards);
    BlockIO executeToDatabase(const ASTRenameQuery & rename, const RenameDescriptions & descriptions);

    enum class RenameType : uint8_t
    {
        RenameTable,
        RenameDatabase
    };

    AccessRightsElements getRequiredAccess(RenameType type) const;

    ASTPtr query_ptr;
    bool renamed_instead_of_exchange{false};
    PreSwapCheck pre_swap_check;
};

}
