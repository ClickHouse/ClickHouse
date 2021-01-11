#pragma once

#include <common/types.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/IAST.h>
#include <Storages/IStorage_fwd.h>
#include <Databases/IDatabase.h>
#include <mutex>


/// General functionality for several different database engines.

namespace DB
{

class Context;

/// A base class for databases that manage their own list of tables.
class DatabaseWithOwnTablesBase : public IDatabase
{
public:
    bool isTableExist(const String & table_name, const Context & context) const override;

    StoragePtr tryGetTable(const String & table_name, const Context & context) const override;

    bool empty() const override;

    void attachTable(const String & table_name, const StoragePtr & table, const String & relative_table_path) override;

    StoragePtr detachTable(const String & table_name) override;

    DatabaseTablesIteratorPtr getTablesIterator(const Context & context, const FilterByNameFunction & filter_by_table_name) override;

    void shutdown() override;

    ~DatabaseWithOwnTablesBase() override;

protected:
    Tables tables;
    Poco::Logger * log;
    const Context & global_context;

    DatabaseWithOwnTablesBase(const String & name_, const String & logger, const Context & context);

    void attachTableUnlocked(const String & table_name, const StoragePtr & table, std::unique_lock<std::mutex> & lock);
    StoragePtr detachTableUnlocked(const String & table_name, std::unique_lock<std::mutex> & lock);
    StoragePtr getTableUnlocked(const String & table_name, std::unique_lock<std::mutex> & lock) const;
};

}
