#pragma once

#include <Interpreters/StorageID.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

template <typename LogElement>
class SystemLogStorage: private boost::noncopyable, protected WithContext
{
public:
    using LogElements = std::vector<LogElement>;

    SystemLogStorage(ContextPtr context_, const String & database, const String & table, const String & engine, bool prepare = false);

    /** Creates new table if it does not exist.
      * Renames old table if its structure is not suitable.
      * This cannot be done in constructor to avoid deadlock while renaming a table under locked Context when SystemLog object is created.
      */
    void prepareTable();

    /** Write records to the table.
      */
    void add(const LogElement & element);
    void add(const LogElements & elements);

    /** Calls flushAndShutdown() for the underlying table.
      */
    void flushAndShutdown();

private:
    ASTPtr getCreateTableQuery();

protected:
    Poco::Logger * log;
    const StorageID table_id;
    const String storage_def;
    String create_query;
    String old_create_query;
    bool is_prepared = false;
};

ASTPtr getCreateTableQueryClean(const StorageID & table_id, ContextPtr context);
void validateEngineDefinition(const String & engine, const String & description = {});

}
