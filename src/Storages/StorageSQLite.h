#pragma once

#include "config.h"

#if USE_SQLITE
#include <Storages/StorageWithCommonVirtualColumns.h>

#include <sqlite3.h>

namespace Poco
{
class Logger;
}

namespace DB
{

class StorageSQLite final : public StorageWithCommonVirtualColumns, public WithContext
{
public:
    using SQLitePtr = std::shared_ptr<sqlite3>;

    StorageSQLite(
        const StorageID & table_id_,
        SQLitePtr sqlite_db_,
        const String & database_path_,
        const String & remote_table_name_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment,
        ContextPtr context_);

    std::string getName() const override { return "SQLite"; }

    static VirtualColumnsDescription createVirtuals();

    using StorageWithCommonVirtualColumns::read;

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr context, bool async_insert) override;

    static ColumnsDescription getTableStructureFromData(
        const SQLitePtr & sqlite_db_,
        const String & table);

private:
    friend class SQLiteSink; /// for write_context

    String remote_table_name;
    String database_path;
    SQLitePtr sqlite_db;
    LoggerPtr log;
    ContextPtr write_context;
};

}

#endif
