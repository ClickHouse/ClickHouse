#pragma once

#include <Databases/IDatabase.h>
#include <Interpreters/IInterpreter.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{
class Context;
using DatabaseAndTable = std::pair<DatabasePtr, StoragePtr>;
class AccessRightsElements;

/** Allow to either drop table with all its data (DROP),
  * or remove information about table (just forget) from server (DETACH),
  * or just clear all data in table (TRUNCATE).
  */
class InterpreterDropQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterDropQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_);

    /// Drop table or database.
    BlockIO execute() override;

    /// `skip_sync_wait` executes a synchronous drop (`sync == true` also makes the dropped table
    /// eligible for the final drop immediately) without waiting for the table to be finally
    /// dropped. It is used for dropping inner tables: the caller waits for them after the DDL
    /// guard of the parent table is released (see `IStorage::getInnerTableIds`), because waiting
    /// under that guard can deadlock with a query which holds references to the inner tables
    /// and blocks on the guard.
    static void executeDropQuery(ASTDropQuery::Kind kind, ContextPtr global_context, ContextPtr current_context,
                                 const StorageID & target_table_id, bool sync, bool ignore_sync_setting = false, bool need_ddl_guard = false,
                                 bool skip_sync_wait = false);

    bool supportsTransactions() const override;

    void extendQueryLogElemImpl(QueryLogElement & elem, const ASTPtr & ast, ContextPtr context_) const override;

private:
    AccessRightsElements getRequiredAccessForDDLOnCluster() const;
    ASTPtr query_ptr;
    ASTPtr current_query_ptr;

    /// See the comment for `executeDropQuery`.
    bool skip_sync_wait = false;

    BlockIO executeSingleDropQuery(const ASTPtr & drop_query_ptr);
    BlockIO executeToDatabase(const ASTDropQuery & query);
    BlockIO executeToDatabaseImpl(const ASTDropQuery & query, DatabasePtr & database, std::vector<UUID> & uuids_to_wait);

    BlockIO executeToTable(ASTDropQuery & query);
    BlockIO executeToTableImpl(const ContextPtr& context_, ASTDropQuery & query, DatabasePtr & db, std::vector<UUID> & uuids_to_wait);

    static void waitForTableToBeActuallyDroppedOrDetached(const ASTDropQuery & query, const DatabasePtr & db, const UUID & uuid_to_wait, ContextPtr context_);

    BlockIO executeToDictionary(const String & database_name, const String & dictionary_name, ASTDropQuery::Kind kind, bool if_exists, bool is_temporary, bool no_ddl_lock);

    BlockIO executeToTemporaryTable(const String & table_name, ASTDropQuery::Kind kind);
};
}
