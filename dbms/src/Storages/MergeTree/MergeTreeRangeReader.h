#pragma once
#include <Core/Block.h>
#include <common/logger_useful.h>

namespace DB
{

class MergeTreeReader;
// void MergeTreeReader::readRange(size_t from_mark, bool is_first_mark_in_range, size_t max_rows_to_read, Block & res);

class MergeTreeRangeReader
{
public:
    size_t unreadRows() const {
        return (last_mark - current_mark) * index_granularity - read_rows_after_current_mark;
    }
    size_t unreadRowsInCurrentPart() const {
        return index_granularity - read_rows_after_current_mark;
    }

    size_t skipToNextMark();
    const MergeTreeRangeReader skipRows(size_t rows) const;
    size_t read(Block & res, size_t max_rows_to_read);
    bool isReadingFinished() const { return is_reading_finished; }

    ~MergeTreeRangeReader() {
        //if (last_mark != current_mark)
        //    LOG_ERROR(logger, "last_mark = " << last_mark << " current_mark = " << current_mark << " read_rows_after_current_mark  = " << read_rows_after_current_mark);
    }

    MergeTreeRangeReader copyForReader(MergeTreeReader & reader);

private:
    MergeTreeRangeReader(MergeTreeReader & merge_tree_reader, size_t from_mark, size_t to_mark, size_t index_granularity);

    Poco::Logger * logger;

    std::reference_wrapper<MergeTreeReader> merge_tree_reader;
    size_t current_mark;
    size_t last_mark;
    size_t read_rows_after_current_mark;
    size_t index_granularity;
    bool seek_to_from_mark;
    bool is_reading_finished;

    friend class MergeTreeReader;
};

}
