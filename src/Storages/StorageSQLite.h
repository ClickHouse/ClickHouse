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
        ContextPtr context_,
        bool generated_columns_reclassification_pending_);

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

    /// Repair the pending generated-column classification before the interpreters freeze the query's metadata
    /// snapshot (this hook is called right before `getInMemoryMetadataPtr`), so even the first query after the
    /// database file becomes reachable - including an `INSERT` - is planned against the corrected metadata.
    void updateExternalDynamicMetadataIfExists(ContextPtr query_context) override;

    static ColumnsDescription getTableStructureFromData(
        const SQLitePtr & sqlite_db_,
        const TableNameOrQuery & table_or_query);

private:
    /// Re-derive the generated-column classification from the remote schema observed through `connection`, when
    /// it could not be applied at construction time because the database file or table was unavailable. Runs at
    /// most once. `connection` is a freshly opened connection on `database_path`, so it observes the current
    /// database file even after a same-path replacement. See the constructor and
    /// `generated_columns_reclassification_pending`.
    void reclassifyGeneratedColumnsFromRemote(ContextPtr query_context, sqlite3 * connection);

    TableNameOrQuery remote_table_or_query;
    String database_path;
    LoggerPtr log;

    /// No SQLite connection is retained by the storage. The connection passed to the constructor is used only
    /// for construction-time schema inference; `read`, `write` and `updateExternalDynamicMetadataIfExists` each
    /// open a fresh connection on `database_path`. A retained handle would keep the file it was opened on: after
    /// a same-path replacement of the database file (`mv new.sqlite data.sqlite`), it would pin the old, unlinked
    /// file on disk for the whole lifetime of the storage, while every query already runs against the replacement.

    /// True while the generated-column classification of an explicitly declared column list still has to be
    /// re-derived from the remote schema because the database file or table schema was unavailable when the
    /// storage was constructed. It is repaired lazily once the remote schema is observed, from
    /// `updateExternalDynamicMetadataIfExists` (before the query's metadata snapshot is taken) and, as a fallback,
    /// from `read`/`write`.
    std::atomic<bool> generated_columns_reclassification_pending{false};
    std::mutex reclassify_mutex;
};

}

#endif
