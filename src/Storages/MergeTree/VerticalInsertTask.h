#pragma once

#include <Core/Block.h>
#include <Core/NamesAndTypes.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergedBlockOutputStream.h>
#include <Storages/MergeTree/MergedColumnOnlyOutputStream.h>
#include <Storages/Statistics/Statistics.h>
#include <Common/Logger.h>

#include <deque>
#include <unordered_map>
#include <unordered_set>

namespace DB
{

/// Performs vertical (column-by-column) insert to reduce memory usage
/// on wide tables. Mirrors vertical merge architecture.
///
/// Two-phase write:
/// 1. Horizontal phase: Write PK/sorting key columns with permutation, then release them from block
/// 2. Vertical phase: Write remaining columns in batches, releasing each batch after writing
///
/// This reduces writer-side memory during serialization (after the input block is materialized)
/// from O(rows * all_columns) to roughly O(rows * batch_size) by progressively releasing columns
/// from the original block after they are written to disk.
class VerticalInsertTask
{
public:
    struct Settings
    {
        size_t columns_batch_size;
        size_t columns_batch_bytes;
    };

    VerticalInsertTask(
        MergeTreeData & data_,
        MergeTreeSettingsPtr data_settings_,
        const StorageMetadataPtr & metadata_snapshot_,
        MergeTreeMutableDataPartPtr new_data_part_,
        Block block_,
        const IColumn::Permutation * permutation_,
        MergeTreeIndices skip_indexes_,
        ColumnsStatistics statistics_,
        CompressionCodecPtr codec_,
        MergeTreeIndexGranularityPtr index_granularity_,
        const Settings & settings_,
        ContextPtr context_);

    /// Execute the two-phase write
    void execute();

    /// Cancel in-flight streams and clean up on failure
    void cancel() noexcept;

    /// Get the output stream for finalization (transfers ownership)
    std::unique_ptr<MergedBlockOutputStream> releaseOutputStream() { return std::move(horizontal_output); }

    /// Get checksums accumulated from vertical phase
    MergeTreeData::DataPart::Checksums & getVerticalChecksums() { return vertical_checksums; }

    /// Get column substreams accumulated from vertical phase
    ColumnsSubstreams & getVerticalColumnsSubstreams() { return vertical_columns_substreams; }

    /// Total number of column streams created during vertical phase
    size_t getVerticalStreamsCreated() const { return vertical_streams_created; }

private:
    struct DelayedColumnStream
    {
        std::unique_ptr<MergedColumnOnlyOutputStream> stream;
        MergeTreeData::DataPart::Checksums checksums;
        ColumnsSubstreams substreams;
        size_t open_streams = 0;
    };

    struct VerticalPhaseState;

    /// Classify columns into merging (PK) and gathering (rest)
    void classifyColumns();

    /// Phase 1: Write PK/sorting key columns
    void executeHorizontalPhase();

    /// Phase 2: Write remaining columns in batches
    void executeVerticalPhase();
    void finalizeDelayedStream(DelayedColumnStream & delayed, bool need_sync);
    void buildOffsetGroups(const NamesAndTypesList & storage_columns);
    void addIndexExpressionColumns(
        const MergeTreeIndexPtr & index,
        Block & batch_block,
        std::unordered_map<String, size_t> & batch_expression_usage);
    void releaseIndexExpressionColumns(const std::unordered_map<String, size_t> & batch_expression_usage);

    MergeTreeData & data;
    MergeTreeSettingsPtr data_settings;
    StorageMetadataPtr metadata_snapshot;
    MergeTreeMutableDataPartPtr new_data_part;
    Block block;
    size_t block_rows;
    size_t block_bytes;
    const IColumn::Permutation * permutation;
    bool has_permutation;
    MergeTreeIndices skip_indexes;
    ColumnsStatistics statistics;
    CompressionCodecPtr codec;
    MergeTreeIndexGranularityPtr index_granularity;
    Settings settings;
    ContextPtr context;

    /// Column classification
    NamesAndTypesList merging_columns;
    NamesAndTypesList gathering_columns;
    std::vector<NamesAndTypesList> offset_groups;
    std::unordered_map<String, size_t> offset_group_by_column;
    NameSet storage_column_names;
    std::unordered_map<String, size_t> index_expression_refcount;

    /// Skip indexes that can be built per-column (single-column indexes)
    std::unordered_map<String, MergeTreeIndices> skip_indexes_by_column;
    /// Skip indexes that must be built in horizontal phase (multi-column and key columns)
    MergeTreeIndices horizontal_skip_indexes;

    /// Statistics that can be built per-column
    std::unordered_map<String, ColumnsStatistics> statistics_by_column;
    /// Statistics that should be built in horizontal phase
    ColumnsStatistics merging_statistics;

    /// State for horizontal phase output
    std::unique_ptr<MergedBlockOutputStream> horizontal_output;
    MergedColumnOnlyOutputStream * active_column_writer = nullptr;

    /// Checksums accumulated from vertical phase
    MergeTreeData::DataPart::Checksums vertical_checksums;
    ColumnsSubstreams vertical_columns_substreams;
    std::unordered_map<String, std::vector<String>> vertical_substreams_by_column;

    /// Column names in original part order
    Names all_column_names;

    std::deque<DelayedColumnStream> delayed_streams;

    size_t vertical_streams_created = 0;

    LoggerPtr log;
};

/// Check if vertical insert should be used for the given block
bool shouldUseVerticalInsert(
    const Block & block,
    const NamesAndTypesList & storage_columns,
    const MergeTreeData::MergingParams & merging_params,
    const MergeTreeIndices & skip_indexes,
    const MergeTreeSettingsPtr & settings,
    const StorageMetadataPtr & metadata_snapshot,
    MergeTreeData::DataPart::Type part_type);

/// Count the number of gathering columns (non-PK columns)
size_t countGatheringColumns(
    const NamesAndTypesList & storage_columns,
    const MergeTreeData::MergingParams & merging_params,
    const StorageMetadataPtr & metadata_snapshot,
    const MergeTreeIndices & skip_indexes);

}
