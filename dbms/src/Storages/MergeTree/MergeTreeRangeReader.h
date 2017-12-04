#pragma once
#include <Core/Block.h>
#include <common/logger_useful.h>

namespace DB
{

class MergeTreeReader;

/// MergeTreeReader iterator which allows sequential reading for arbitrary number of rows between pairs of marks in the same part.
/// Stores reading state, which can be inside granule. Can skip rows in current granule and start reading from next mark.
/// Used generally for reading number of rows less than index granularity to decrease cache misses for fat blocks.
class MergeTreeRangeReader
{
public:
    size_t numPendingRows() const { return (last_mark - current_mark) * index_granularity - read_rows_after_current_mark; }
    size_t numPendingRowsInCurrentGranule() const { return index_granularity - read_rows_after_current_mark; }

    size_t numReadRowsInCurrentGranule() const { return read_rows_after_current_mark; }

    /// Seek to next mark before next reading.
    size_t skipToNextMark();
    /// Seturn state will be afrer reading rows_to_read, no reading happens.
    MergeTreeRangeReader getFutureState(size_t rows_to_read) const;

    /// If columns are not present in the block, adds them. If they are present - appends the values that have been read.
    /// Do not add columns, if the files are not present for them.
    /// Block should contain either no columns from the columns field, or all columns for which files are present.
    /// Returns the number of rows was read.
    size_t read(Block & res, size_t max_rows_to_read);

    bool isReadingFinished() const { return is_reading_finished; }

    void disableNextSeek() { continue_reading = true; }
    /// Return the same state for other MergeTreeReader.
    MergeTreeRangeReader copyForReader(MergeTreeReader & reader);

private:
    MergeTreeRangeReader(MergeTreeReader & merge_tree_reader, size_t from_mark, size_t to_mark, size_t index_granularity);

    std::reference_wrapper<MergeTreeReader> merge_tree_reader;
    size_t current_mark;
    size_t last_mark;
    size_t read_rows_after_current_mark = 0;
    size_t index_granularity;
    bool continue_reading = false;
    bool is_reading_finished = false;

    friend class MergeTreeReader;
};

}

