#pragma once

#include <Storages/StorageWithCommonVirtualColumns.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>


namespace DB
{

class Context;
class QueryStatus;

struct StoragesInfo
{
    StoragePtr storage = nullptr;
    TableLockHolder table_lock;

    String database;
    String table;
    String engine;

    bool need_inactive_parts = false;
    MergeTreeData * data = nullptr;

    explicit operator bool() const { return storage != nullptr; }

    /// If `query_status` is provided, the part enumeration periodically checks for query cancellation
    /// and time limits, and if the time limit is exceeded in the 'break' mode, returns the partial result.
    MergeTreeData::DataPartsVector getParts(MergeTreeData::DataPartStateVector & state, bool has_state_column, const std::shared_ptr<QueryStatus> & query_status) const;
    MergeTreeData::ProjectionPartsVector getProjectionParts(MergeTreeData::DataPartStateVector & state, bool has_state_column, const std::shared_ptr<QueryStatus> & query_status) const;
};

/** A helper class that enumerates the storages that match given query. */
class StoragesInfoStreamBase
{
public:
    explicit StoragesInfoStreamBase(ContextPtr context);

    StoragesInfoStreamBase(const StoragesInfoStreamBase&) = default;
    virtual ~StoragesInfoStreamBase() = default;

    StoragesInfo next();

protected:
    virtual bool tryLockTable(StoragesInfo & info);

    String query_id;
    std::chrono::milliseconds lock_timeout;

    /// Enumerating the storages can be slow, so we check for query cancellation and time limits.
    std::shared_ptr<QueryStatus> query_status;

    ColumnPtr database_column;
    ColumnPtr table_column;
    ColumnPtr active_column;
    ColumnPtr storage_uuid_column;

    size_t next_row;
    size_t rows;

    using StoragesMap = std::unordered_map<UUID, StoragePtr>;
    StoragesMap storages;
};


class StoragesInfoStream : public StoragesInfoStreamBase
{
public:
    StoragesInfoStream(std::optional<ActionsDAG> filter_by_database, std::optional<ActionsDAG> filter_by_other_columns, ContextPtr context);
};

/** Implements system table 'parts' which allows to get information about data parts for tables of MergeTree family.
  */
class StorageSystemPartsBase : public StorageWithCommonVirtualColumns
{
public:
    void readImpl(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    static VirtualColumnsDescription createVirtuals();

    bool isSystemStorage() const override { return true; }

private:
    static bool hasStateColumn(const Names & column_names, const StorageSnapshotPtr & storage_snapshot);

protected:
    friend class ReadFromSystemPartsBase;

    StorageSystemPartsBase(const StorageID & table_id_, ColumnsDescription && columns);

    virtual std::unique_ptr<StoragesInfoStreamBase> getStoragesInfoStream(std::optional<ActionsDAG> filter_by_database, std::optional<ActionsDAG> filter_by_other_columns, ContextPtr context)
    {
        return std::make_unique<StoragesInfoStream>(std::move(filter_by_database), std::move(filter_by_other_columns), context);
    }

    virtual void
    processNextStorage(ContextPtr context, MutableColumns & columns, std::vector<UInt8> & columns_mask, const StoragesInfo & info, bool has_state_column) = 0;
};

}
