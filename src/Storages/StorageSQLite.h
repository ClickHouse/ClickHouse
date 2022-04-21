#pragma once

#include "config_core.h"

#if USE_SQLITE
#include <Storages/IStorage.h>

#include <sqlite3.h>

namespace Poco
{
class Logger;
}

namespace DB
{

class StorageSQLite final : public IStorage, public WithContext
{
private:
    struct CreatePasskey
    {
    };

public:
    template <typename... TArgs>
    static std::shared_ptr<StorageSQLite> create(TArgs &&... args)
    {
        return std::make_shared<StorageSQLite>(CreatePasskey{}, std::forward<TArgs>(args)...);
    }

    template <typename... TArgs>
    explicit StorageSQLite(CreatePasskey, TArgs &&... args) : StorageSQLite{std::forward<TArgs>(args)...}
    {
    }

    using SQLitePtr = std::shared_ptr<sqlite3>;

    std::string getName() const override { return "SQLite"; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr context) override;

private:
    StorageSQLite(
        const StorageID & table_id_,
        SQLitePtr sqlite_db_,
        const String & database_path_,
        const String & remote_table_name_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        ContextPtr context_);

    String remote_table_name;
    String database_path;
    SQLitePtr sqlite_db;
    Poco::Logger * log;
};

}

#endif
