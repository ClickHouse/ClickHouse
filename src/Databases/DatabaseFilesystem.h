#pragma once

#include <mutex>
#include <Databases/IDatabase.h>
#include <Parsers/IAST.h>
#include <Storages/IStorage_fwd.h>
#include <base/types.h>

namespace DB
{

class Context;

/**
  * DatabaseFilesystem allows to interact with files stored on the file system
  * Uses TableFunctionFile to implicitly load file when a user requests the table, and provides read-only access to the data in the file
  * Tables are cached inside the database for quick access
  *
  * Used in clickhouse-local to access local files
  */
class DatabaseFilesystem : public IDatabase, protected WithContext
{
public:
    DatabaseFilesystem(const String & name, const String & path, ContextPtr context);

    String getEngineName() const override { return "Filesystem"; }

    bool isTableExist(const String & name, ContextPtr context) const override;

    StoragePtr tryGetTable(const String & name, ContextPtr context) const override;

    bool empty() const override { return true; }

    ASTPtr getCreateDatabaseQuery() const override;

    void shutdown() override;

    std::vector<std::pair<ASTPtr, StoragePtr>> getTablesForBackup(const FilterByNameFunction &, const ContextPtr &) const override;
    DatabaseTablesIteratorPtr getTablesIterator(ContextPtr, const FilterByNameFunction &) const override;

protected:
    std::string getTablePath(const std::string & table_name) const;
    void addTable(const std::string & table_name, StoragePtr table_storage) const;

private:
    String path;
    mutable Tables loaded_tables TSA_GUARDED_BY(mutex);
    Poco::Logger * log;
};

} // DB
