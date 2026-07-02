#pragma once

#include "config.h"

#if USE_SQLITE
#include <Storages/StorageWithCommonVirtualColumns.h>
#include <Storages/TableNameOrQuery.h>

#include <sqlite3.h>

#include <atomic>
#include <mutex>

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
        const TableNameOrQuery & remote_table_or_query_,
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
        const TableNameOrQuery & table_or_query);

private:
    friend class SQLiteSink; /// for write_context

    /// Re-derive the generated-column classification from the remote schema on the first successful open,
    /// when it could not be applied at construction time because the database file was unavailable. See the
    /// constructor and `generated_columns_reclassification_pending`.
    void reclassifyGeneratedColumnsFromRemote(ContextPtr query_context);

    TableNameOrQuery remote_table_or_query;
    String database_path;
    SQLitePtr sqlite_db;
    LoggerPtr log;
    ContextPtr write_context;

    /// True while the generated-column classification of an explicitly declared column list still has to be
    /// re-derived from the remote schema because the database file was unavailable when the storage was
    /// constructed (e.g. a persisted `SQLite` table attached on startup while the file is temporarily
    /// missing). It is repaired lazily on the first successful open in `read`/`write`.
    std::atomic<bool> generated_columns_reclassification_pending{false};
    std::mutex reclassify_mutex;
};

}

#endif
