#pragma once

#include <Storages/IStorage.h>
#include <Storages/SelectQueryInfo.h>
#include <QueryPipeline/Pipe.h>


namespace DB
{

class StorageProxy : public IStorage
{
public:

    explicit StorageProxy(const StorageID & table_id_) : IStorage(table_id_) {}

    virtual StoragePtr getNested() const = 0;

    /// The wrapped storage if it already exists, or null. Never creates it, so an observer
    /// iterating every table cannot trigger a load.
    virtual StoragePtr tryGetNested() const { return nullptr; }

    /// Whether the proxy only defers the creation of the storage and adds no behaviour of its own,
    /// so that an operation may run on the wrapped storage directly.
    virtual bool isLazyStandIn() const { return false; }

    String getName() const override { return "Proxy"; }

    bool isRemote() const override { return getNested()->isRemote(); }
    std::vector<StoragePtr> getUnderlyingStorages() const override { return getNested()->getUnderlyingStorages(); }
    bool isView() const override { return getNested()->isView(); }
    bool supportsTruncate() const override { return getNested()->supportsTruncate(); }
    bool supportsSampling() const override { return getNested()->supportsSampling(); }
    bool supportsFinal() const override { return getNested()->supportsFinal(); }
    bool supportsPrewhere() const override { return getNested()->supportsPrewhere(); }
    bool canMoveConditionsToPrewhere() const override { return getNested()->canMoveConditionsToPrewhere(); }
    std::optional<NameSet> supportedPrewhereColumns() const override { return getNested()->supportedPrewhereColumns(); }
    bool supportedPrewhereColumnsIncludeSubcolumns() const override { return getNested()->supportedPrewhereColumnsIncludeSubcolumns(); }
    bool supportsReplication() const override { return getNested()->supportsReplication(); }
    bool supportsParallelInsert() const override { return getNested()->supportsParallelInsert(); }
    bool supportsDeduplication() const override { return getNested()->supportsDeduplication(); }
    bool noPushingToViewsOnInserts() const override { return getNested()->noPushingToViewsOnInserts(); }
    bool hasEvenlyDistributedRead() const override { return getNested()->hasEvenlyDistributedRead(); }
    bool supportsSubcolumns() const override { return getNested()->supportsSubcolumns(); }
    /// The IStorage default ties this to supportsSubcolumns(); forward it so a proxy around a
    /// storage that opts out of the rewrite (e.g. Distributed) does not re-advertise true.
    bool supportsOptimizationToSubcolumns() const override { return getNested()->supportsOptimizationToSubcolumns(); }
    bool supportsOptimizationToTupleElementSubcolumns() const override { return getNested()->supportsOptimizationToTupleElementSubcolumns(); }
    bool supportsColumnsWithDynamicStructure() const override { return getNested()->supportsColumnsWithDynamicStructure(); }
    /// `ReadFromMerge::getSelectedTables` prunes children by name based on this flag; a lazy
    /// `StorageTableProxy` around a delegating storage (`Distributed`, `Merge`, `Buffer`, `Alias`)
    /// answering false would let a `_table`/`_database` filter incorrectly prune the child.
    bool readsFromOtherTables() const override { return getNested()->readsFromOtherTables(); }
    size_t getMaxReadStreams(size_t num_streams, ContextPtr context) override { return getNested()->getMaxReadStreams(num_streams, context); }
    /// `AlterCommands::validate` checks these on the storage the ALTER is addressed to, which is
    /// the proxy itself for lazily loaded tables — forward them so support does not depend on the
    /// database's `lazy_load_tables` setting. Both are only queried while validating an ALTER,
    /// which materializes the nested table anyway.
    bool supportsTTL() const override { return getNested()->supportsTTL(); }
    bool supportsStatistics() const override { return getNested()->supportsStatistics(); }

    ColumnSizeByName getColumnSizes() const override { return getNested()->getColumnSizes(); }
    ColumnSizeByName getColumnSizes(const Names & columns, bool calculate_subcolumn_sizes) const override { return getNested()->getColumnSizes(columns, calculate_subcolumn_sizes); }

    StorageSnapshotPtr getStorageSnapshot(const StorageMetadataPtr & base_metadata, ContextPtr query_context) const override
    {
        auto nested_metadata = getNested()->getInMemoryMetadataPtr(query_context, false);
        auto new_metadata = std::make_shared<StorageInMemoryMetadata>(base_metadata->withVirtuals(nested_metadata->virtuals));
        return std::make_shared<StorageSnapshot>(*this, std::move(new_metadata));
    }

    QueryProcessingStage::Enum getQueryProcessingStage(
        ContextPtr context,
        QueryProcessingStage::Enum to_stage,
        const StorageSnapshotPtr &,
        SelectQueryInfo & info) const override
    {
        const auto nested_metadata = getNested()->getInMemoryMetadataPtr(context, false);
        return getNested()->getQueryProcessingStage(context, to_stage, getNested()->getStorageSnapshot(nested_metadata, context), info);
    }

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override
    {
        getNested()->read(query_plan, column_names, storage_snapshot, query_info, context, processed_stage, max_block_size, num_streams);
    }

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context, bool async_insert) override
    {
        return getNested()->write(query, metadata_snapshot, context, async_insert);
    }

    void checkInsertIsAllowed(ContextPtr context) const override { getNested()->checkInsertIsAllowed(context); }

    void drop() override { getNested()->drop(); }

    void truncate(
        const ASTPtr & query,
        const StorageMetadataPtr & metadata_snapshot,
        ContextPtr context,
        TableExclusiveLockHolder & lock) override
    {
        getNested()->truncate(query, metadata_snapshot, context, lock);
    }

    void rename(const String & new_path_to_table_data, const StorageID & new_table_id) override
    {
        getNested()->rename(new_path_to_table_data, new_table_id);
        IStorage::renameInMemory(new_table_id);
    }

    void renameInMemory(const StorageID & new_table_id) override
    {
        getNested()->renameInMemory(new_table_id);
        IStorage::renameInMemory(new_table_id);
    }

    void alter(const AlterCommands & params, ContextPtr context, AlterLockHolder & alter_lock_holder, DDLGuardPtr & ddl_guard) override
    {
        getNested()->alter(params, context, alter_lock_holder, ddl_guard);
        auto nested_metadata = getNested()->getInMemoryMetadataPtr(context, true);
        IStorage::setInMemoryMetadata(*nested_metadata);
    }

    void checkAlterIsPossible(const AlterCommands & commands, ContextPtr context) const override
    {
        getNested()->checkAlterIsPossible(commands, context);
    }

    Pipe alterPartition(
            const StorageMetadataPtr & metadata_snapshot,
            const PartitionCommands & commands,
            ContextPtr context) override
    {
        return getNested()->alterPartition(metadata_snapshot, commands, context);
    }

    void checkAlterPartitionIsPossible(const PartitionCommands & commands, const StorageMetadataPtr & metadata_snapshot, const Settings & settings, ContextPtr context) const override
    {
        getNested()->checkAlterPartitionIsPossible(commands, metadata_snapshot, settings, context);
    }

    bool optimize(
            const ASTPtr & query,
            const StorageMetadataPtr & metadata_snapshot,
            const ASTPtr & partition,
            bool final,
            bool deduplicate,
            const Names & deduplicate_by_columns,
            bool cleanup,
            ContextPtr context) override
    {
        return getNested()->optimize(query, metadata_snapshot, partition, final, deduplicate, deduplicate_by_columns, cleanup, context);
    }

    void mutate(const MutationCommands & commands, ContextPtr context) override { getNested()->mutate(commands, context); }

    CancellationCode killMutation(const String & mutation_id) override { return getNested()->killMutation(mutation_id); }

    void startup() override { getNested()->startup(); }
    void shutdown(bool is_drop) override { getNested()->shutdown(is_drop); }
    void flushAndPrepareForShutdown() override { getNested()->flushAndPrepareForShutdown(); }

    ActionLock getActionLock(StorageActionBlockType action_type) override { return getNested()->getActionLock(action_type); }

    DataValidationTasksPtr getCheckTaskList(const CheckTaskFilter & check_task_filter, ContextPtr context) override
    {
        return getNested()->getCheckTaskList(check_task_filter, context);
    }

    std::optional<CheckResult> checkDataNext(DataValidationTasksPtr & check_task_list) override
    {
        return getNested()->checkDataNext(check_task_list);
    }

    void checkTableCanBeDropped([[ maybe_unused ]] ContextPtr query_context) const override { getNested()->checkTableCanBeDropped(query_context); }
    void checkTableSizeBelowDropLimit([[ maybe_unused ]] ContextPtr query_context) const override { getNested()->checkTableSizeBelowDropLimit(query_context); }

    bool storesDataOnDisk() const override { return getNested()->storesDataOnDisk(); }
    Strings getDataPaths() const override { return getNested()->getDataPaths(); }
    StoragePolicyPtr getStoragePolicy() const override { return getNested()->getStoragePolicy(); }
    std::optional<UInt64> totalRows(ContextPtr query_context) const override { return getNested()->totalRows(query_context); }
    std::optional<UInt64> totalBytes(ContextPtr query_context) const override { return getNested()->totalBytes(query_context); }
    std::optional<UInt64> lifetimeRows() const override { return getNested()->lifetimeRows(); }
    std::optional<UInt64> lifetimeBytes() const override { return getNested()->lifetimeBytes(); }

};

/// The storage an operation should run on: the lazy stand-in is replaced by the storage it wraps
/// once that exists, while the other proxies add behaviour of their own and are kept.
inline StoragePtr resolveStorageProxy(const StoragePtr & storage)
{
    const auto * proxy = dynamic_cast<const StorageProxy *>(storage.get());
    if (!proxy || !proxy->isLazyStandIn())
        return storage;
    auto nested = proxy->tryGetNested();
    return nested ? nested : storage;
}

/// Same, but creates the wrapped storage when it does not exist yet. For operations that name a
/// table explicitly, where loading it is the expected cost of the operation.
inline StoragePtr resolveStorageProxyLoading(const StoragePtr & storage)
{
    const auto * proxy = dynamic_cast<const StorageProxy *>(storage.get());
    return proxy && proxy->isLazyStandIn() ? proxy->getNested() : storage;
}

/// Proxies stack: a lazily loaded `URL` table is a `StorageTableProxy` over a `StorageURLSchemeDispatch`
/// over the real storage. The bound only guards against a cycle.
constexpr size_t max_storage_proxy_depth = 16;

/// What a cast does with a table that is not loaded yet.
enum class DeferredTable : uint8_t
{
    /// Load it. For an operation that names the table, where loading is its expected cost.
    Load,
    /// Leave it unloaded, so the cast yields null and the caller skips it. For an observer that
    /// walks every table and must not turn a listing into a load.
    Skip,
};

/// The single way to cast a catalog pointer to a concrete engine type. A lazily loaded table is
/// reached through `StorageTableProxy`, so a direct cast fails even once the table is loaded.
template <typename T>
std::shared_ptr<T> castStorage(const StoragePtr & storage, DeferredTable deferred_table)
{
    /// The type test looks through every layer, whichever wrappers sit on top of the engine.
    StoragePtr resolved = storage;
    for (size_t depth = 0; depth < max_storage_proxy_depth && resolved; ++depth)
    {
        const auto * proxy = dynamic_cast<const StorageProxy *>(resolved.get());
        if (!proxy)
            break;
        auto nested = deferred_table == DeferredTable::Load ? proxy->getNested() : proxy->tryGetNested();
        if (!nested)
            break;
        resolved = nested;
    }
    return std::dynamic_pointer_cast<T>(resolved);
}

}
