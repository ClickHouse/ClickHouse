#pragma once

#include <vector>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <boost/core/noncopyable.hpp>
#include <Core/NamesAndTypes.h>
#include <Storages/StorageSnapshot.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/AlterConversions.h>
#include <Storages/MergeTree/MergeTreeReadersChain.h>
#include <Storages/MergeTree/PatchParts/MergeTreePatchReader.h>
#include <Storages/MergeTree/MergeTreeIndices.h>

namespace DB
{

class UncompressedCache;
class MarkCache;

struct MergeTreeBlockSizePredictor;
using MergeTreeBlockSizePredictorPtr = std::shared_ptr<MergeTreeBlockSizePredictor>;

class IMergeTreeDataPart;
using DataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;
using MergeTreeReaderPtr = std::unique_ptr<IMergeTreeReader>;

class DeserializationPrefixesCache;
using DeserializationPrefixesCachePtr = std::shared_ptr<DeserializationPrefixesCache>;

class MergedPartOffsets;
using MergedPartOffsetsPtr = std::shared_ptr<MergedPartOffsets>;

struct MergeTreeIndexReadResult;
using MergeTreeIndexReadResultPtr = std::shared_ptr<MergeTreeIndexReadResult>;

struct MergeTreeIndexBuildContext;
using MergeTreeIndexBuildContextPtr = std::shared_ptr<MergeTreeIndexBuildContext>;

enum class MergeTreeReadType : uint8_t
{
    /// By default, read will use MergeTreeReadPool and return pipe with num_streams outputs.
    /// If num_streams == 1, will read without pool, in order specified in parts.
    Default,
    /// Read in sorting key order.
    /// Returned pipe will have the number of ports equals to parts.size().
    /// Parameter num_streams_ is ignored in this case.
    /// User should add MergingSorted itself if needed.
    InOrder,
    /// The same as InOrder, but in reverse order.
    /// For every part, read ranges and granules from end to begin. Also add ReverseTransform.
    InReverseOrder,
    /// A special type of reading where every replica
    /// talks to a remote coordinator (which is located on the initiator node)
    /// and who spreads marks and parts across them.
    ParallelReplicas,
};

/// Read task for index.
/// Some indexes (e.g. inverted text index) may read special virtual columns.
struct IndexReadTask
{
    NamesAndTypesList columns;
    MergeTreeIndexWithCondition index;
};

using IndexReadTasks = std::unordered_map<String, IndexReadTask>;
using IndexReadColumns = std::unordered_map<String, NamesAndTypesList>;

struct MergeTreeReadTaskColumns
{
    /// Column names to read during WHERE
    NamesAndTypesList columns;
    /// Column names to read during each PREWHERE step
    std::vector<NamesAndTypesList> pre_columns;
    /// Column names to read from patch parts.
    std::vector<NamesAndTypesList> patch_columns;

    String dump() const;
    Names getAllColumnNames() const;
    void moveAllColumnsFromPrewhere();
};

struct MergeTreeReadTaskInfo
{
    /// Data part which should be read while performing this task
    DataPartPtr data_part;
    /// Parent part of the projection part
    DataPartPtr parent_part;
    /// For `part_index` virtual column
    size_t part_index_in_query;
    /// For `part_starting_offset` virtual column
    size_t part_starting_offset_in_query;
    /// Alter converversionss that should be applied on-fly for part.
    AlterConversionsPtr alter_conversions;
    /// `_part_offset` mapping used to merge projections with `_part_offset`.
    MergedPartOffsetsPtr merged_part_offsets;
    /// Prewhere steps that should be applied to execute on-fly mutations for part.
    PrewhereExprSteps mutation_steps;
    /// Patches that should be applied for part.
    PatchPartsForReader patch_parts;
    /// Column names to read during PREWHERE and WHERE
    MergeTreeReadTaskColumns task_columns;
    /// Shared initialized size predictor. It is copied for each new task.
    MergeTreeBlockSizePredictorPtr shared_size_predictor;
    /// Shared constant fields for virtual columns.
    VirtualFields const_virtual_fields;
    /// Index read tasks.
    IndexReadTasks index_read_tasks;
    /// The amount of data to read per task based on size of the queried columns.
    size_t min_marks_per_task = 0;
    size_t approx_size_of_mark = 0;
    /// Cache of the columns prefixes for this part.
    DeserializationPrefixesCachePtr deserialization_prefixes_cache;
    /// Extra info for optimizations - exact row processing, calculated virtual columns.
    RangesInDataPartReadHints read_hints;
};

using MergeTreeReadTaskInfoPtr = std::shared_ptr<const MergeTreeReadTaskInfo>;

/// A batch of work for MergeTreeSelectProcessor
struct MergeTreeReadTask : private boost::noncopyable
{
public:
    /// Extra params that required for creation of reader.
    struct Extras
    {
        UncompressedCache * uncompressed_cache = nullptr;
        MarkCache * mark_cache = nullptr;
        PatchJoinCache * patch_join_cache = nullptr;
        MergeTreeReaderSettings reader_settings{};
        StorageSnapshotPtr storage_snapshot{};
        ValueSizeMap value_size_map{};
        ReadBufferFromFileBase::ProfileCallback profile_callback{};
    };

    struct Readers
    {
        MergeTreeReaderPtr main;
        std::vector<MergeTreeReaderPtr> prewhere;
        MergeTreePatchReaders patches;
        MergeTreeReaderPtr prepared_index;

        void updateAllMarkRanges(const MarkRanges & ranges);
    };

    struct BlockSizeParams
    {
        UInt64 max_block_size_rows = DEFAULT_BLOCK_SIZE;
        UInt64 preferred_block_size_bytes = 1000000;
        UInt64 preferred_max_column_in_block_size_bytes = 0;
        double min_filtration_ratio = 0.00001;
    };

    /// The result of reading from task.
    struct BlockAndProgress
    {
        Block block;
        MarkRanges read_mark_ranges;
        size_t row_count = 0;
        size_t num_read_rows = 0;
        size_t num_read_bytes = 0;
    };

    MergeTreeReadTask(
        MergeTreeReadTaskInfoPtr info_,
        Readers readers_,
        MarkRanges mark_ranges_,
        std::vector<MarkRanges> patches_mark_ranges_,
        const BlockSizeParams & block_size_params_,
        MergeTreeBlockSizePredictorPtr size_predictor_);

    void initializeReadersChain(
        const PrewhereExprInfo & prewhere_actions,
        MergeTreeIndexBuildContextPtr index_build_context,
        const ReadStepsPerformanceCounters & read_steps_performance_counters);

    void initializeIndexReader(const MergeTreeIndexBuildContext & index_build_context);

    BlockAndProgress read();
    bool isFinished() const { return mark_ranges.empty() && readers_chain.isCurrentRangeFinished(); }

    const MergeTreeReadTaskInfo & getInfo() const { return *info; }
    const MergeTreeReadersChain & getReadersChain() const { return readers_chain; }
    const IMergeTreeReader & getMainReader() const { return *readers.main; }

    void addPrewhereUnmatchedMarks(const MarkRanges & mark_ranges_);
    const MarkRanges & getPrewhereUnmatchedMarks() { return prewhere_unmatched_marks; }

    Readers releaseReaders() { return std::move(readers); }

    size_t getNumMarksToRead() const { return mark_ranges.getNumberOfMarks(); }

    static Readers createReaders(
        const MergeTreeReadTaskInfoPtr & read_info,
        const Extras & extras,
        const MarkRanges & ranges,
        const std::vector<MarkRanges> & patches_ranges);

    static MergeTreeReadersChain createReadersChain(
        const Readers & readers,
        const PrewhereExprInfo & prewhere_actions,
        const ReadStepsPerformanceCounters & read_steps_performance_counters);

private:
    UInt64 estimateNumRows() const;

    /// Shared information required for reading.
    MergeTreeReadTaskInfoPtr info;

    /// Readers for data_part of this task.
    /// May be reused and released to the next task.
    Readers readers;

    /// Range readers chain to read mark_ranges from data_part.
    MergeTreeReadersChain readers_chain;

    /// Ranges to read from data_part.
    MarkRanges mark_ranges;

    /// Ranges to read from patch parts.
    std::vector<MarkRanges> patches_mark_ranges;

    /// Tracks which mark ranges are not matched by PREWHERE (needed for query condition cache)
    MarkRanges prewhere_unmatched_marks;

    BlockSizeParams block_size_params;

    /// Used to satistfy preferred_block_size_bytes limitation
    MergeTreeBlockSizePredictorPtr size_predictor;
};

using MergeTreeReadTaskPtr = std::unique_ptr<MergeTreeReadTask>;

}
