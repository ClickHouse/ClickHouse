#pragma once

#include <boost/core/noncopyable.hpp>
#include <Core/NamesAndTypes.h>
#include <Storages/StorageSnapshot.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <Storages/MergeTree/AlterConversions.h>
#include <Storages/MergeTree/MergeTreeReadersChain.h>

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

struct MergeTreeReadTaskColumns
{
    /// Column names to read during WHERE
    NamesAndTypesList columns;
    /// Column names to read during each PREWHERE step
    std::vector<NamesAndTypesList> pre_columns;

    String dump() const;
    void moveAllColumnsFromPrewhere();
};

struct MergeTreeReadTaskInfo
{
    bool hasLightweightDelete() const;

    /// Data part which should be read while performing this task
    DataPartPtr data_part;
    /// Parent part of the projection part
    DataPartPtr parent_part;
    /// For `part_index` virtual column
    size_t part_index_in_query;
    /// Alter converversionss that should be applied on-fly for part.
    AlterConversionsPtr alter_conversions;
    /// Prewhere steps that should be applied to execute on-fly mutations for part.
    PrewhereExprSteps mutation_steps;
    /// Column names to read during PREWHERE and WHERE
    MergeTreeReadTaskColumns task_columns;
    /// Shared initialized size predictor. It is copied for each new task.
    MergeTreeBlockSizePredictorPtr shared_size_predictor;
    /// Shared constant fields for virtual columns.
    VirtualFields const_virtual_fields;
    /// The amount of data to read per task based on size of the queried columns.
    size_t min_marks_per_task = 0;
    size_t approx_size_of_mark = 0;
    /// Cache of the columns prefixes for this part.
    DeserializationPrefixesCachePtr deserialization_prefixes_cache{};
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
        MergeTreeReaderSettings reader_settings{};
        StorageSnapshotPtr storage_snapshot{};
        ValueSizeMap value_size_map{};
        ReadBufferFromFileBase::ProfileCallback profile_callback{};
    };

    struct Readers
    {
        MergeTreeReaderPtr main;
        std::vector<MergeTreeReaderPtr> prewhere;
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
        const BlockSizeParams & block_size_params_,
        MergeTreeBlockSizePredictorPtr size_predictor_);

    void initializeReadersChain(const PrewhereExprInfo & prewhere_actions, ReadStepsPerformanceCounters & read_steps_performance_counters);

    BlockAndProgress read();
    bool isFinished() const { return mark_ranges.empty() && readers_chain.isCurrentRangeFinished(); }

    const MergeTreeReadTaskInfo & getInfo() const { return *info; }
    const MergeTreeReadersChain & getReadersChain() const { return readers_chain; }
    const IMergeTreeReader & getMainReader() const { return *readers.main; }

    void addPrewhereUnmatchedMarks(const MarkRanges & mark_ranges_);
    const MarkRanges & getPrewhereUnmatchedMarks() { return prewhere_unmatched_marks; }

    Readers releaseReaders() { return std::move(readers); }

    static Readers createReaders(const MergeTreeReadTaskInfoPtr & read_info, const Extras & extras, const MarkRanges & ranges);
    static MergeTreeReadersChain createReadersChain(const Readers & readers, const PrewhereExprInfo & prewhere_actions, ReadStepsPerformanceCounters & read_steps_performance_counters);

private:
    UInt64 estimateNumRows() const;

    /// Shared information required for reading.
    MergeTreeReadTaskInfoPtr info;

    /// Readers for data_part of this task.
    /// May be reused and released to the next task.
    Readers readers;

    /// Range readers chain to read mark_ranges from data_part
    MergeTreeReadersChain readers_chain;

    /// Ranges to read from data_part
    MarkRanges mark_ranges;

    /// Tracks which mark ranges are not matched by PREWHERE (needed for query condition cache)
    MarkRanges prewhere_unmatched_marks;

    BlockSizeParams block_size_params;

    /// Used to satistfy preferred_block_size_bytes limitation
    MergeTreeBlockSizePredictorPtr size_predictor;
};

using MergeTreeReadTaskPtr = std::unique_ptr<MergeTreeReadTask>;

}
