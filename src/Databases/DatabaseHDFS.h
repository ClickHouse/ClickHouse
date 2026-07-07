#pragma once

#include "config.h"

#if USE_HDFS

#include <mutex>
#include <Databases/IDatabase.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/IStorage_fwd.h>
#include <base/types.h>

namespace DB
{

class Context;

/**
  * DatabaseHDFS allows to interact with files stored on the file system.
  * Uses TableFunctionHDFS to implicitly load file when a user requests the table,
  * and provides read-only access to the data in the file.
  * Tables are cached inside the database for quick access.
  */
class DatabaseHDFS : public IDatabase, protected WithContext
{
public:
    DatabaseHDFS(const String & name, const String & source_url, ContextPtr context);

    String getEngineName() const override { return "S3"; }

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
    StoragePtr getTableImpl(const String & name, ContextPtr context) const;

    void addTable(const std::string & table_name, StoragePtr table_storage) const;

    bool checkUrl(const std::string & url, ContextPtr context_, bool throw_on_error) const;

    std::string getTablePath(const std::string & table_name) const;

private:
    const String source;

    mutable Tables loaded_tables TSA_GUARDED_BY(mutex);
    LoggerPtr log;
};

}

#endif
