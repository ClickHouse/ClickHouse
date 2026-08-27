#pragma once

#include <Storages/StorageWithCommonVirtualColumns.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>


namespace DB
{

class Context;
class QueryStatus;

/// How often the column-oriented `system.parts` siblings consult `QueryStatus` while enumerating
/// the columns of a part: a wide table can have many thousands of columns per part, and polling
/// only at the part boundary would leave a long uninterruptible stretch.
static constexpr size_t COLUMNS_CANCELLATION_CHECK_PERIOD = 128;

/// Test-only instrumentation, a no-op unless the `slowdown_system_parts_enumeration` failpoint
/// is enabled: sleeps while enumerating the parts of specially named tables, to make the eager
/// result building slow enough for the tests of query cancellation and time limits.
void slowDownSystemPartsEnumeration(const String & table_name);

/// The same, but for the column-enumeration loops: sleeps once per
/// `COLUMNS_CANCELLATION_CHECK_PERIOD` enumerated columns of a part.
void slowDownSystemPartsColumnsEnumeration(const String & table_name, size_t column_position);

/// The same, but for the storage-discovery prepass of `StoragesInfoStream` (the eager walk over
/// all databases and tables): sleeps on every walked table with the matching name, so that the
/// tests can prove that the walk itself stops at its cancellation checkpoint. It is scoped to
/// a narrower table-name prefix than `slowDownSystemPartsEnumeration`, so that the discovery
/// fixture does not slow down the walk for the queries that test the later checkpoints.
void slowDownSystemPartsDiscovery(const String & table_name);

/// The same, but for the column-metadata prepass of the column-oriented tables. It is scoped to
/// a narrower table-name prefix than the loops above, so that the tests can exercise the prepass
/// checkpoints and the later per-part / per-column checkpoints independently: a query over a
/// table slowed down in the prepass never reaches the later loops within its time limit.
void slowDownSystemPartsMetadataEnumeration(const String & table_name, size_t column_position);

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
    /// and time limits, and if the time limit is exceeded in the 'break' mode, stops early.
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
