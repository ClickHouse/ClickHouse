#pragma once

#include <filesystem>

#include <DataTypes/DataTypesNumber.h>
#include <IO/SharedThreadPools.h>
#include <Parsers/ASTIdentifier.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Storages/KeyDescription.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/StorageSnapshot.h>
#include <Common/ThreadStatus.h>
/// Also the source of `Context` here: the style check forbids `Interpreters/Context.h` in a header.
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

namespace DB
{

/// A mutations snapshot with nothing behind it. `counters` is what the logical digest's ruling-7a
/// gate reads, so a test can pin that gate without building real mutations or patch parts.
struct TestMutationsSnapshot : public MergeTreeData::MutationsSnapshotBase
{
    explicit TestMutationsSnapshot(MutationCounters counters_) { counters = counters_; }

    MutationCommands getOnFlyMutationCommandsForPart(const DataPartPtr &) const override { return {}; }
    std::shared_ptr<IMutationsSnapshot> cloneEmpty() const override { return std::make_shared<TestMutationsSnapshot>(counters); }
    NameSet getAllUpdatedColumns() const override { return {}; }
};

/// The smallest storage a `ReadFromMergeTree` can be built over: one `UInt64` column `a`, `ORDER BY a`,
/// no partition key, attached (so no sanity checks) and with no data on disk. Shared by the step
/// identity and the memo deduplication gtests.
struct MergeTreeReadFixture
{
    ContextMutablePtr context;
    std::shared_ptr<StorageMergeTree> storage;
    StorageMetadataPtr metadata_snapshot;
    StorageSnapshotPtr storage_snapshot;
    MergeTreeSettingsPtr data_settings;
    RangesInDataPartsPtr parts;
    String relative_data_path;

    /// `shared_context` lets a second table live in the context of the first, so that a comparison of
    /// two tables varies the table alone. `partition_by_unsorted_column` adds a second column `p` and
    /// partitions by it: a partition key the sorting key does not determine is what makes
    /// `deferFiltersAfterFinalIfNeeded` skip partition pruning under FINAL.
    /// Defined out of line: building the storage settings needs a complete `MergeTreeSettings`, which
    /// only `MergeTreeSettings.h` gives, and the style check forbids that include in a header.
    explicit MergeTreeReadFixture(
        const String & table_name, ContextMutablePtr shared_context = nullptr, bool partition_by_unsorted_column = false);

    ~MergeTreeReadFixture()
    {
        /// Capture the on-disk paths before shutdown, then remove them: `StorageMergeTree` never
        /// deletes its own directory, so a bare `flushAndShutdown` leaves `relative_data_path` behind
        /// on every run.
        const auto data_paths = storage->getDataPaths();
        storage->flushAndShutdown();
        for (const auto & path : data_paths)
            std::filesystem::remove_all(path);
    }

    /// `table_expression_modifiers` must be present: with no modifiers and no query tree `isFinal()`
    /// falls back to the (absent) select AST. Note that every call allocates its own `PreparedSets`,
    /// which the full digest witnesses - reads meant to differ in one field only must share one
    /// `SelectQueryInfo` (copies share the pointer). The logical digest does not witness it, which is
    /// exactly what lets two independently built reads merge.
    static SelectQueryInfo makeQueryInfo()
    {
        SelectQueryInfo query_info;
        query_info.table_expression_modifiers.emplace(/*has_final_=*/ false, std::nullopt, std::nullopt);
        return query_info;
    }

    /// Everything a read is built from that a test may want to vary. A null snapshot or part list
    /// means the fixture's own, which is what the reads of one table normally share; passing separate
    /// objects is the self-join case.
    struct ReadOptions
    {
        SelectQueryInfo query_info = makeQueryInfo();
        StorageSnapshotPtr snapshot;
        RangesInDataPartsPtr parts;
        MergeTreeData::MutationsSnapshotPtr mutations_snapshot;
        PartitionIdToMaxBlockPtr max_block_numbers_to_read;
        Names columns{"a"};
    };

    /// Takes a callback rather than an initializer list: `-Wmissing-designated-field-initializers` is
    /// an error here, so a test cannot set one field of `ReadOptions` and leave the rest defaulted.
    std::unique_ptr<ReadFromMergeTree> makeReadWith(const std::function<void(ReadOptions &)> & tweak) const
    {
        ReadOptions options;
        tweak(options);

        return std::make_unique<ReadFromMergeTree>(
            options.parts ? options.parts : parts,
            options.mutations_snapshot,
            options.columns,
            *storage,
            data_settings,
            options.query_info,
            options.snapshot ? options.snapshot : storage_snapshot,
            context,
            /*max_block_size_=*/ 8192,
            /*num_streams_=*/ 1,
            options.max_block_numbers_to_read,
            getLogger("CascadesStepIdentityTest"),
            /*analyzed_result_ptr_=*/ nullptr,
            /*enable_parallel_reading_=*/ false);
    }

    std::unique_ptr<ReadFromMergeTree> makeRead(const SelectQueryInfo & query_info) const
    {
        return makeReadWith([&](ReadOptions & options) { options.query_info = query_info; });
    }

    /// A snapshot of its own, as a second table expression over the same table gets: a distinct
    /// `StorageSnapshot` object holding the same metadata object.
    StorageSnapshotPtr makeOwnSnapshot() const { return storage->getStorageSnapshotWithoutData(metadata_snapshot, context); }

    /// A read built the way a second table expression over the same table is: its own query info, its
    /// own storage snapshot, its own part-list object. The logical digest sees past all three.
    std::unique_ptr<ReadFromMergeTree> makeIndependentRead() const
    {
        return makeReadWith([&](ReadOptions & options)
        {
            options.snapshot = makeOwnSnapshot();
            options.parts = std::make_shared<RangesInDataParts>();
        });
    }
};

}
