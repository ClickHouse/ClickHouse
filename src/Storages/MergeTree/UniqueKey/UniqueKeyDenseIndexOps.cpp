#include <Storages/MergeTree/UniqueKey/UniqueKeyDenseIndexOps.h>

#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTSelectQuery.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/StorageFromMergeTreeDataPart.h>
#include <Storages/MergeTree/UniqueKey/SSTIndexWriter.h>
#include <Storages/MergeTree/UniqueKey/UniqueKeySSTProbe.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/StorageSnapshot.h>

#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>

#if USE_ROCKSDB
#include <rocksdb/sst_file_reader.h>
#include <rocksdb/status.h>
#include <rocksdb/table_properties.h>
#endif

#include <limits>


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
    extern const int LOGICAL_ERROR;
    extern const int SUPPORT_IS_DISABLED;
    extern const int UNIQUE_KEY_DENSE_INDEX_UNREADABLE;
}

namespace Setting
{
    extern const SettingsNonZeroUInt64 max_block_size;
}


/// ============================================================================
/// Stateless write path — produces the dense index for one part's block.
/// ============================================================================

namespace
{

/// Half-written staging file left by an interrupted `SSTIndexWriter::finalizeToStorage`
/// (mirrors its private `FILE_NAME + ".tmp"` staging name).
const std::string SST_STAGING_FILE_NAME = std::string(SSTIndexWriter::FILE_NAME) + ".tmp";

#if USE_ROCKSDB
/// Outcome of validating an existing `unique_key_index.sst`.
enum class DenseIndexSSTStatus
{
    Valid,      /// opens, checksums verify, entry count matches the part → trust it
    Corrupt,    /// genuine damage (corruption status or wrong entry count) → remove + rebuild
    Transient,  /// could not determine (I/O error / exception) → leave file, fail the load
};

/// Validate an existing `unique_key_index.sst`. Presence is not trust: the SST
/// carries no `checksums.txt` entry. `num_entries == rows_count` holds because
/// `SSTIndexWriter` does one `Put` per row and rejects Nullable UK columns, so an
/// entry-count mismatch flags a block-boundary truncation that still checksum-
/// verifies. `reason` is filled for the log/exception message.
DenseIndexSSTStatus classifyDenseIndexSST(const String & sst_path, UInt64 expected_rows, String & reason)
{
    try
    {
        auto opened = tryOpenSSTReaderFromPath(sst_path);
        if (!opened.status.ok())
        {
            reason = "open failed: " + opened.status.ToString();
            return opened.status.IsCorruption() ? DenseIndexSSTStatus::Corrupt : DenseIndexSSTStatus::Transient;
        }
        auto & reader = *opened.reader;

        auto verify_status = reader.VerifyChecksum();
        if (!verify_status.ok())
        {
            reason = "checksum verify failed: " + verify_status.ToString();
            return verify_status.IsCorruption() ? DenseIndexSSTStatus::Corrupt : DenseIndexSSTStatus::Transient;
        }

        /// Table properties are read during `Open`; after a successful open +
        /// verify they are populated without an extra read.
        auto props = reader.GetTableProperties();
        if (!props)
        {
            reason = "table properties unavailable after open";
            return DenseIndexSSTStatus::Transient;
        }
        if (props->num_entries != expected_rows)
        {
            reason = fmt::format("entry count {} != part rows_count {} (truncated/stale index)",
                                 props->num_entries, expected_rows);
            return DenseIndexSSTStatus::Corrupt;
        }
        return DenseIndexSSTStatus::Valid;
    }
    catch (...)
    {
        reason = "exception: " + getCurrentExceptionMessage(/*with_stacktrace=*/false);
        return DenseIndexSSTStatus::Transient;
    }
}
#endif

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
    /// `unique_key_index.sst`.
    ///
    /// The SST must be a standalone file: every reader (load-time validation and
    /// the probe) opens it by raw filesystem path via RocksDB. On packed part
    /// storage `writeFile` would silently bury it inside the archive and every
    /// subsequent load of the part would fail — fail the INSERT loudly instead.
    /// (`MergeTreeDataWriter` forces Full storage for UNIQUE KEY parts.)
    if (storage.getType() != MergeTreeDataPartStorageType::Full)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "UNIQUE KEY dense index requires full part storage, got part storage type {}",
            storage.getType().toString());
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


void UniqueKeyDenseIndexOps::ensureValidDenseIndex(MutableDataPartPtr & part, bool storage_is_writable) const
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
    {
#if USE_ROCKSDB
        /// An existing SST is not trusted on presence alone (it carries no
        /// checksums.txt entry). Validate it and branch on the outcome.
        ///
        /// Cost: `classifyDenseIndexSST` runs `VerifyChecksum`, which re-reads
        /// every block, so this is O(index-size) per part on every load / ATTACH
        /// — the inherent price of the corruption guarantee for this experimental
        /// feature.
        /// TODO(unique-key): skip re-verification for a known-good SST via a
        /// validity/generation marker written alongside it, so an unchanged file
        /// loads without a full re-read.
        String reason;
        switch (classifyDenseIndexSST(
            storage.getFullPath() + "/" + SSTIndexWriter::FILE_NAME, part->rows_count, reason))
        {
            case DenseIndexSSTStatus::Valid:
                return;
            case DenseIndexSSTStatus::Corrupt:
                /// On readonly storage neither removal nor rebuild (both writes)
                /// is possible; fail the load closed instead of activating an
                /// unprobeable part. The file is left untouched.
                if (!storage_is_writable)
                    throw Exception(ErrorCodes::UNIQUE_KEY_DENSE_INDEX_UNREADABLE,
                        "ensureValidDenseIndex: part {} has a corrupt `{}` ({}) and the "
                        "storage is readonly, so it cannot be rebuilt; failing the load",
                        part->name, SSTIndexWriter::FILE_NAME, reason);
                LOG_WARNING(log, "ensureValidDenseIndex: part {} has a corrupt `{}` ({}); "
                            "removing and rebuilding",
                            part->name, SSTIndexWriter::FILE_NAME, reason);
                storage.removeFileIfExists(SSTIndexWriter::FILE_NAME);
                break;  /// fall through to the rebuild path below
            case DenseIndexSSTStatus::Transient:
                /// Do not delete or rebuild: the file may be healthy but momentarily
                /// unreadable. Fail the load with a distinguished code the caller
                /// re-raises (instead of detaching the part as broken) so a
                /// retry/restart can recover the untouched file.
                throw Exception(ErrorCodes::UNIQUE_KEY_DENSE_INDEX_UNREADABLE,
                    "ensureValidDenseIndex: could not validate `{}` for part {} ({}); "
                    "leaving it in place and failing the load for retry",
                    SSTIndexWriter::FILE_NAME, part->name, reason);
        }
#else
        /// Without RocksDB we cannot open/validate the SST; presence is all we
        /// can check (and the probe path is unavailable anyway). Keep the part.
        return;
#endif
    }

    /// From here on a rebuild (a write) is required. On readonly storage fail
    /// the load closed — the part must not activate without a probeable index,
    /// and detaching (a rename) is not possible either.
    if (!storage_is_writable)
        throw Exception(ErrorCodes::UNIQUE_KEY_DENSE_INDEX_UNREADABLE,
            "ensureValidDenseIndex: part {} is missing `{}` and the storage is "
            "readonly, so it cannot be rebuilt; failing the load",
            part->name, SSTIndexWriter::FILE_NAME);

    /// Fail closed if any UK column is missing from the part: a non-empty UK
    /// part with no dense index would let duplicate keys slip past the probe.
    /// The caller detaches the part as broken rather than activating it.
    const auto & part_cols = part->getColumns();
    for (const auto & uk_name : uk_names)
    {
        if (!part_cols.tryGetByName(uk_name).has_value())
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "ensureValidDenseIndex: part {} is missing UK column '{}'; cannot rebuild dense index",
                part->name, uk_name);
    }

    try
    {
#if USE_ROCKSDB
        Stopwatch rebuild_watch;
        Block accumulated = readUniqueKeyColumns(part, metadata_snapshot, uk_names);
        if (accumulated.rows() == 0)
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "ensureValidDenseIndex: part {} has rows_count={} but sequential read yielded 0 rows; "
                "cannot rebuild dense index",
                part->name, part->rows_count);

        const UInt64 rows = accumulated.rows();
        /// `unique_key_max_encoded_size` is an INSERT-time ingestion policy (a
        /// check-only bound in `encodeBlock`); the rebuild re-encodes rows the
        /// server already accepted at INSERT, so no cap applies here.
        SSTIndexWriter::write(
            storage,
            accumulated,
            uk_names,
            metadata_snapshot->getSortingKeyColumns(),
            metadata_snapshot->getSortingKeyReverseFlags(),
            /*permutation=*/nullptr,
            /*max_encoded_size=*/std::numeric_limits<UInt64>::max(),
            data.getContext());

        const UInt64 elapsed_us = rebuild_watch.elapsedMicroseconds();
        ProfileEvents::increment(ProfileEvents::UniqueKeyLoadTimeSSTRebuildCount);
        ProfileEvents::increment(ProfileEvents::UniqueKeyLoadTimeSSTRebuildMicroseconds, elapsed_us);

        LOG_INFO(log, "ensureValidDenseIndex: rebuilt `{}` for part {} ({} rows, {} us)",
                 SSTIndexWriter::FILE_NAME, part->name, rows, elapsed_us);
#else
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "ensureValidDenseIndex: part {} needs a UNIQUE KEY dense index but the server was built without RocksDB",
            part->name);
#endif
    }
    catch (...)
    {
        /// A rebuild that throws leaves the part without a usable dense index;
        /// surface it to the caller so the part is detached as broken rather
        /// than activated. (Re-throw the original error.)
        tryLogCurrentException(log,
            "ensureValidDenseIndex: SST rebuild failed for part " + part->name);
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
    /// Attach paths write (the part was just moved/created), so the storage is
    /// writable by construction.
    ensureValidDenseIndex(part, /*storage_is_writable=*/true);
}


#if USE_ROCKSDB
Block UniqueKeyDenseIndexOps::readUniqueKeyColumns(
    const MutableDataPartPtr & part,
    const StorageMetadataPtr & metadata_snapshot,
    const Names & uk_names) const
{
    /// Read the part's UK columns through a single-part storage view — the same
    /// `StorageFromMergeTreeDataPart` path projection materialization and MutateTask
    /// use. Force an unmasked read: the dense index must map every physical row's
    /// `_part_offset`, so `apply_deleted_mask=false` keeps the `_row_exists`
    /// lightweight-delete filter out (deadness is tracked by the DeleteBitmap).
    auto context = Context::createCopy(data.getContext());
    context->setSetting("apply_deleted_mask", false);
    auto mutations_snapshot = data.getMutationsSnapshot({});
    auto storage_from_part = std::make_shared<StorageFromMergeTreeDataPart>(
        std::const_pointer_cast<const IMergeTreeDataPart>(part), mutations_snapshot);

    auto storage_snapshot = storage_from_part->getStorageSnapshot(metadata_snapshot, context);

    QueryPlan plan;
    SelectQueryInfo query_info;
    /// The `ReadFromMergeTree` ctor calls `SelectQueryInfo::isFinal()`, which
    /// dereferences `query` when neither `query_tree` nor
    /// `table_expression_modifiers` is set — so a plain (non-FINAL) `ASTSelectQuery`
    /// is required here.
    query_info.query = make_intrusive<ASTSelectQuery>();
    const size_t max_block_size = context->getSettingsRef()[Setting::max_block_size];
    storage_from_part->read(
        plan, uk_names, storage_snapshot, query_info, context,
        QueryProcessingStage::FetchColumns, max_block_size, /*num_streams=*/1);

    /// An uninitialized plan means the storage produced no reader (nothing to
    /// read). Return an empty block; the caller fails closed on a non-empty part.
    if (!plan.isInitialized())
        return storage_snapshot->getSampleBlockForColumns(uk_names).cloneEmpty();

    auto builder = plan.buildQueryPipeline(
        QueryPlanOptimizationSettings(context), BuildQueryPipelineSettings(context));
    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));

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
