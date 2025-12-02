#pragma once

#include "config.h"

#if USE_AWS_S3

#include <mutex>
#include <Databases/IDatabase.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/IStorage_fwd.h>
#include <base/types.h>

namespace DB
{

class Context;

/**
  * DatabaseS3 provides access to data stored in S3.
  * Uses TableFunctionS3 to implicitly load file when a user requests the table,
  * and provides read-only access to the data in the file.
  * Tables are cached inside the database for quick access.
  */
class DatabaseS3 : public IDatabase, protected WithContext
{
public:
    struct Configuration
    {
        std::string url_prefix;

        bool no_sign_request = false;

        std::optional<std::string> access_key_id;
        std::optional<std::string> secret_access_key;
    };

    DatabaseS3(const String & name, const Configuration& config, ContextPtr context);

    String getEngineName() const override { return "S3"; }

    bool isTableExist(const String & name, ContextPtr context) const override;

    StoragePtr getTable(const String & name, ContextPtr context) const override;

    StoragePtr tryGetTable(const String & name, ContextPtr context) const override;

    // Contains only temporary tables
    bool shouldBeEmptyOnDetach() const override { return false; }

    bool empty() const override;

    bool isReadOnly() const override { return true; }

    ASTPtr getCreateDatabaseQuery() const override;

    void shutdown() override;

    std::vector<std::pair<ASTPtr, StoragePtr>> getTablesForBackup(const FilterByNameFunction &, const ContextPtr &) const override;
    DatabaseTablesIteratorPtr getTablesIterator(ContextPtr, const FilterByNameFunction &, bool) const override;

    static Configuration parseArguments(ASTs engine_args, ContextPtr context);

protected:
    StoragePtr getTableImpl(const String & name, ContextPtr context) const;

    void addTable(const std::string & table_name, StoragePtr table_storage) const;

    bool checkUrl(const std::string & url, ContextPtr context_, bool throw_on_error) const;

    std::string getFullUrl(const std::string & name) const;

private:
    const Configuration config;

    mutable Tables loaded_tables TSA_GUARDED_BY(mutex);
    LoggerPtr log;
};

}

#endif
