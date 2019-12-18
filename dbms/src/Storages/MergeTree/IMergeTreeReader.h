#pragma once

#include <Core/NamesAndTypes.h>
#include <Storages/MergeTree/MergeTreeReaderStream.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>


namespace DB
{

class IDataType;

/// Reads the data between pairs of marks in the same part. When reading consecutive ranges, avoids unnecessary seeks.
/// When ranges are almost consecutive, seeks are fast because they are performed inside the buffer.
/// Avoids loading the marks file if it is not needed (e.g. when reading the whole part).
class IMergeTreeReader : private boost::noncopyable
{
public:
    using ValueSizeMap = std::map<std::string, double>;
    using DeserializeBinaryBulkStateMap = std::map<std::string, IDataType::DeserializeBinaryBulkStatePtr>;

    IMergeTreeReader(const MergeTreeData::DataPartPtr & data_part_,
        const NamesAndTypesList & columns_,
        UncompressedCache * uncompressed_cache_,
        MarkCache * mark_cache_,
        const MarkRanges & all_mark_ranges_,
        const MergeTreeReaderSettings & settings_,
        const ValueSizeMap & avg_value_size_hints_ = ValueSizeMap{});

    /// Return the number of rows has been read or zero if there is no columns to read.
    /// If continue_reading is true, continue reading from last state, otherwise seek to from_mark
    virtual size_t readRows(size_t from_mark, bool continue_reading, size_t max_rows_to_read, Block & res) = 0;

    virtual bool canReadIncompleteGranules() const = 0;

    virtual ~IMergeTreeReader();

    const ValueSizeMap & getAvgValueSizeHints() const;

    /// Add columns from ordered_names that are not present in the block.
    /// Missing columns are added in the order specified by ordered_names.
    /// If at least one column was added, reorders all columns in the block according to ordered_names.
    /// num_rows is needed in case block is empty.
    void fillMissingColumns(Block & res, bool & should_reorder, bool & should_evaluate_missing_defaults, size_t num_rows);
    /// Sort columns to ensure consistent order among all blocks.
    /// If filter_name is not nullptr and block has filter column, move it to the end of block.
    void reorderColumns(Block & res, const Names & ordered_names, const String * filter_name);
    /// Evaluate defaulted columns if necessary.
    void evaluateMissingDefaults(Block & res);

    const NamesAndTypesList & getColumns() const { return columns; }

    size_t getFirstMarkToRead() const
    {
        return all_mark_ranges.back().begin;
    }

    MergeTreeData::DataPartPtr data_part;

protected:

    /// avg_value_size_hints are used to reduce the number of reallocations when creating columns of variable size.
    ValueSizeMap avg_value_size_hints;
    /// Stores states for IDataType::deserializeBinaryBulk
    DeserializeBinaryBulkStateMap deserialize_binary_bulk_state_map;
    /// Path to the directory containing the part
    String path;

    /// Columns that are read.
    NamesAndTypesList columns;

    UncompressedCache * uncompressed_cache;
    MarkCache * mark_cache;
    /// If save_marks_in_cache is false, then, if marks are not in cache, we will load them but won't save in the cache, to avoid evicting other data.

    MergeTreeReaderSettings settings;

    const MergeTreeData & storage;
    MarkRanges all_mark_ranges;

    friend class MergeTreeRangeReader::DelayedStream;
};

}
