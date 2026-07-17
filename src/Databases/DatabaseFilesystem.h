#pragma once

#include <mutex>
#include <Databases/IDatabase.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/IStorage_fwd.h>
#include <base/types.h>

namespace DB
{

class Context;

/**
  * DatabaseFilesystem allows to interact with files stored on the local filesystem.
  * Uses TableFunctionFile to implicitly load file when a user requests the table,
  * and provides a read-only access to the data in the file.
  * Tables are cached inside the database for quick access
  *
  * Used in clickhouse-local to access local files.
  * For clickhouse-server requires allows to access file only from user_files directory.
  */
class DatabaseFilesystem : public IDatabase, protected WithContext
{
public:
    DatabaseFilesystem(const String & name, const String & path, ContextPtr context);

    String getEngineName() const override { return "Filesystem"; }

    bool isTableExist(const String & name, ContextPtr context) const override;

    StoragePtr getTable(const String & name, ContextPtr context) const override;

    StoragePtr tryGetTable(const String & name, ContextPtr context) const override;

    bool shouldBeEmptyOnDetach() const override { return false; } /// Contains only temporary tables.

    bool empty() const override;

    bool isReadOnly() const override { return true; }

    ASTPtr getCreateDatabaseQuery() const override;

    void shutdown() override;

    std::vector<std::pair<ASTPtr, StoragePtr>> getTablesForBackup(const FilterByNameFunction &, const ContextPtr &) const override;

    DatabaseTablesIteratorPtr getTablesIterator(ContextPtr, const FilterByNameFunction &, bool) const override;

protected:
    StoragePtr getTableImpl(const String & name, ContextPtr context, bool throw_on_error) const;

    StoragePtr tryGetTableFromCache(const std::string & name) const;

    std::string getTablePath(const std::string & table_name) const;

    void addTable(const std::string & table_name, StoragePtr table_storage) const;

    bool checkTableFilePath(const std::string & table_path, ContextPtr context_, bool throw_on_error) const;

private:
    String path;
    mutable Tables loaded_tables TSA_GUARDED_BY(mutex);
    LoggerPtr log;
};

}
