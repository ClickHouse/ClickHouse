#include <Storages/MergeTree/UniqueKey/UniqueKeyDenseIndexOps.h>

#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/UniqueKey/SSTIndexWriter.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/StorageSnapshot.h>
#include <Storages/MergeTree/AlterConversions.h>

#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>


namespace ProfileEvents
{
    extern const Event UniqueKeyLoadTimeSSTRebuildCount;
    extern const Event UniqueKeyLoadTimeSSTRebuildMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int SUPPORT_IS_DISABLED;
}

namespace Setting
{
    extern const SettingsUInt64 unique_key_max_encoded_size;
}


/// ============================================================================
/// Stateless write path — produces the dense index for one part's block.
/// ============================================================================

namespace
{

/// Half-written staging file left by an interrupted `SSTIndexWriter::finalizeToStorage`
/// (mirrors its private `FILE_NAME + ".tmp"` staging name).
const std::string SST_STAGING_FILE_NAME = std::string(SSTIndexWriter::FILE_NAME) + ".tmp";

}


void UniqueKeyDenseIndexOps::writeDenseIndexOnInsert(
    IDataPartStorage & storage,
    const StorageMetadataPtr & metadata_snapshot,
    const Block & block,
    const IColumn::Permutation * permutation,
    UInt64 max_encoded_size,
    ContextPtr context)
{
    /// Caller (`MergeTreeDataWriter`) ensures the table has a UNIQUE KEY.
    /// `SSTIndexWriter` accounts for `UniqueKeySSTWriteMicroseconds` itself, and
    /// throws SUPPORT_IS_DISABLED without RocksDB: a UNIQUE KEY INSERT that cannot
    /// build the dense index fails closed rather than publishing a part with no
    /// `unique_key_index.sst`. (UK-INSERT stateless tests carry `no-fasttest`.)
    SSTIndexWriter::write(
        storage,
        block,
        metadata_snapshot->getUniqueKeyColumns(),
        metadata_snapshot->getSortingKeyColumns(),
        metadata_snapshot->getSortingKeyReverseFlags(),
        permutation,
        max_encoded_size,
        context);
}


/// ============================================================================
/// Per-storage load lifecycle — orphan sweep + load-time rebuild over parts.
/// ============================================================================

void UniqueKeyDenseIndexOps::sweepOrphans(const DataPartsLock & /*part_lock*/)
{
    /// SST-side sweep only. Delete-bitmap recovery + version GC live in
    /// the txn commit/recovery protocol, not here.
    auto & log = data.log;
    auto metadata_snapshot = data.getInMemoryMetadataPtr(data.getContext(), /*bypass_metadata_cache=*/false);
    const bool table_has_uk = metadata_snapshot && metadata_snapshot->hasUniqueKey();

    size_t removed_stray_ssts = 0;
    size_t removed_tmp_ssts = 0;

    for (const auto & part : data.data_parts_by_info)
    {
        if (part->getState() != MergeTreeData::DataPartState::Active)
            continue;

        auto & storage = const_cast<IMergeTreeDataPart &>(*part).getDataPartStorage();

        if (!table_has_uk && storage.existsFile(SSTIndexWriter::FILE_NAME))
        {
            LOG_WARNING(log, "loadDataParts: removing stray '{}' from part '{}' (table has no UNIQUE KEY)",
                        SSTIndexWriter::FILE_NAME, part->name);
            storage.removeFileIfExists(SSTIndexWriter::FILE_NAME);
            ++removed_stray_ssts;
        }

        if (storage.existsFile(SST_STAGING_FILE_NAME))
        {
            LOG_WARNING(log, "loadDataParts: removing half-written '{}' from part '{}'",
                        SST_STAGING_FILE_NAME, part->name);
            storage.removeFileIfExists(SST_STAGING_FILE_NAME);
            ++removed_tmp_ssts;
        }
    }

    if (removed_stray_ssts || removed_tmp_ssts)
        LOG_INFO(log, "loadDataParts: unique-key SST sweep removed {} stray + {} half-written file(s)",
                 removed_stray_ssts, removed_tmp_ssts);
}


void UniqueKeyDenseIndexOps::rebuildIfMissing(MutableDataPartPtr & part) const
{
    if (!part || part->rows_count == 0)
        return;

    auto & log = data.log;
    auto metadata_snapshot = data.getInMemoryMetadataPtr(data.getContext(), /*bypass_metadata_cache=*/false);
    if (!metadata_snapshot || !metadata_snapshot->hasUniqueKey())
        return;

    const auto & uk_names = metadata_snapshot->getUniqueKeyColumns();
    if (uk_names.empty())
        return;

    auto & storage = part->getDataPartStorage();
    if (storage.existsFile(SSTIndexWriter::FILE_NAME))
        return;

    /// Fail closed if any UK column is missing from the part: a non-empty UK
    /// part with no dense index would let duplicate keys slip past the probe.
    /// The caller detaches the part as broken rather than activating it.
    const auto & part_cols = part->getColumns();
    for (const auto & uk_name : uk_names)
    {
        if (!part_cols.tryGetByName(uk_name).has_value())
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "rebuildIfMissing: part {} is missing UK column '{}'; cannot rebuild dense index",
                part->name, uk_name);
    }

    try
    {
#if USE_ROCKSDB
        Stopwatch rebuild_watch;
        Block accumulated = readUniqueKeyColumns(part, metadata_snapshot, uk_names);
        if (accumulated.rows() == 0)
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "rebuildIfMissing: part {} has rows_count={} but sequential read yielded 0 rows; "
                "cannot rebuild dense index",
                part->name, part->rows_count);

        const UInt64 rows = accumulated.rows();
        const auto max_encoded_size = data.getContext()->getSettingsRef()[Setting::unique_key_max_encoded_size];
        SSTIndexWriter::write(
            storage,
            accumulated,
            uk_names,
            metadata_snapshot->getSortingKeyColumns(),
            metadata_snapshot->getSortingKeyReverseFlags(),
            /*permutation=*/nullptr,
            max_encoded_size,
            data.getContext());

        const UInt64 elapsed_us = rebuild_watch.elapsedMicroseconds();
        ProfileEvents::increment(ProfileEvents::UniqueKeyLoadTimeSSTRebuildCount);
        ProfileEvents::increment(ProfileEvents::UniqueKeyLoadTimeSSTRebuildMicroseconds, elapsed_us);

        LOG_INFO(log, "rebuildIfMissing: rebuilt `{}` for part {} ({} rows, {} us)",
                 SSTIndexWriter::FILE_NAME, part->name, rows, elapsed_us);
#else
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "rebuildIfMissing: part {} needs a UNIQUE KEY dense index but the server was built without RocksDB",
            part->name);
#endif
    }
    catch (...)
    {
        /// A rebuild that throws leaves the part without a usable dense index;
        /// surface it to the caller so the part is detached as broken rather
        /// than activated. (Re-throw the original error.)
        tryLogCurrentException(log,
            "rebuildIfMissing: SST rebuild failed for part " + part->name);
        throw;
    }
}


void UniqueKeyDenseIndexOps::onPartAttach(MutableDataPartPtr & part) const
{
    if (!part)
        return;
    auto & storage = part->getDataPartStorage();
    if (storage.existsFile(SST_STAGING_FILE_NAME))
    {
        LOG_WARNING(data.log, "onPartAttach: removing half-written `{}` from part {}",
                    SST_STAGING_FILE_NAME, part->name);
        storage.removeFileIfExists(SST_STAGING_FILE_NAME);
    }
    rebuildIfMissing(part);
}


#if USE_ROCKSDB
Block UniqueKeyDenseIndexOps::readUniqueKeyColumns(
    const MutableDataPartPtr & part,
    const StorageMetadataPtr & metadata_snapshot,
    const Names & uk_names) const
{
    RangesInDataPart ranges(part);
    auto storage_snapshot = std::make_shared<StorageSnapshot>(data, metadata_snapshot);
    auto empty_alter_conversions = std::make_shared<const AlterConversions>();

    auto pipe = createMergeTreeSequentialSource(
        MergeTreeSequentialSourceType::Mutation,
        data,
        storage_snapshot,
        std::move(ranges),
        empty_alter_conversions,
        /*merged_part_offsets=*/nullptr,
        uk_names,
        /*mark_ranges=*/std::nullopt,
        /*filtered_rows_count=*/nullptr,
        /*apply_deleted_mask=*/false,
        /*read_with_direct_io=*/false,
        /*prefetch=*/false);

    QueryPipeline pipeline(std::move(pipe));
    /// Snapshot the header before binding to the executor — once bound, the
    /// pipeline transitions out of the state `getHeader()` asserts on.
    const auto pipeline_header = pipeline.getHeader();
    PullingPipelineExecutor executor(pipeline);

    MutableColumns accum_columns = pipeline_header.cloneEmptyColumns();
    Block chunk;
    while (executor.pull(chunk))
    {
        if (chunk.rows() == 0)
            continue;
        for (size_t c = 0; c < chunk.columns(); ++c)
            accum_columns[c]->insertRangeFrom(*chunk.getByPosition(c).column, 0, chunk.rows());
    }

    Block accumulated = pipeline_header.cloneEmpty();
    for (size_t c = 0; c < accum_columns.size(); ++c)
        accumulated.getByPosition(c).column = std::move(accum_columns[c]);
    return accumulated;
}
#endif

}
