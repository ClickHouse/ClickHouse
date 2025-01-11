#pragma once

#include <boost/core/noncopyable.hpp>
#include <Core/NamesAndTypes.h>
#include <Storages/StorageSnapshot.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <Storages/MergeTree/AlterConversions.h>

namespace DB
{

class UncompressedCache;
class MarkCache;

struct MergeTreeBlockSizePredictor;
using MergeTreeBlockSizePredictorPtr = std::shared_ptr<MergeTreeBlockSizePredictor>;

class IMergeTreeDataPart;
using DataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;
using MergeTreeReaderPtr = std::unique_ptr<IMergeTreeReader>;
using VirtualFields = std::unordered_map<String, Field>;


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
};

struct MergeTreeReadTaskInfo
{
    /// Data part which should be read while performing this task
    DataPartPtr data_part;
    /// Parent part of the projection part
    DataPartPtr parent_part;
    /// For `part_index` virtual column
    size_t part_index_in_query;
    /// Alter converversionss that should be applied on-fly for part.
    AlterConversionsPtr alter_conversions;
    /// Column names to read during PREWHERE and WHERE
    MergeTreeReadTaskColumns task_columns;
    /// Shared initialized size predictor. It is copied for each new task.
    MergeTreeBlockSizePredictorPtr shared_size_predictor;
    /// Shared constant fields for virtual columns.
    VirtualFields const_virtual_fields;
    /// The amount of data to read per task based on size of the queried columns.
    size_t min_marks_per_task = 0;
    size_t approx_size_of_mark = 0;
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
        IMergeTreeReader::ValueSizeMap value_size_map{};
        ReadBufferFromFileBase::ProfileCallback profile_callback{};
    };

    struct Readers
    {
        MergeTreeReaderPtr main;
        std::vector<MergeTreeReaderPtr> prewhere;
    };

    struct RangeReaders
    {
        /// Used to save current range processing status
        MergeTreeRangeReader main;

        /// Range readers for multiple filtering steps: row level security, PREWHERE etc.
        /// NOTE: we take references to elements and push_back new elements, that's why it is a deque but not a vector
        std::deque<MergeTreeRangeReader> prewhere;
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

    void initializeRangeReaders(const PrewhereExprInfo & prewhere_actions);

    BlockAndProgress read();
    bool isFinished() const { return mark_ranges.empty() && range_readers.main.isCurrentRangeFinished(); }

    const MergeTreeReadTaskInfo & getInfo() const { return *info; }
    const MergeTreeRangeReader & getMainRangeReader() const { return range_readers.main; }
    const IMergeTreeReader & getMainReader() const { return *readers.main; }

    Readers releaseReaders() { return std::move(readers); }

    static Readers createReaders(const MergeTreeReadTaskInfoPtr & read_info, const Extras & extras, const MarkRanges & ranges);
    static RangeReaders createRangeReaders(const Readers & readers, const PrewhereExprInfo & prewhere_actions);

private:
    UInt64 estimateNumRows() const;

    /// Shared information required for reading.
    MergeTreeReadTaskInfoPtr info;

    /// Readers for data_part of this task.
    /// May be reused and released to the next task.
    Readers readers;

    /// Range readers to read mark_ranges from data_part
    RangeReaders range_readers;

    /// Ranges to read from data_part
    MarkRanges mark_ranges;

    BlockSizeParams block_size_params;

    /// Used to satistfy preferred_block_size_bytes limitation
    MergeTreeBlockSizePredictorPtr size_predictor;
};

using MergeTreeReadTaskPtr = std::unique_ptr<MergeTreeReadTask>;

}
