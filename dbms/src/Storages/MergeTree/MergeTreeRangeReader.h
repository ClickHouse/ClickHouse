#pragma once
#include <Core/Block.h>


namespace DB
{

class MergeTreeReader;
// void MergeTreeReader::readRange(size_t from_mark, bool is_first_mark_in_range, size_t max_rows_to_read, Block & res);

class MergeTreeRangeReader
{
public:
    bool read(Block & res, size_t max_rows_to_read);
    // MergeTreeRangeReader & operator=(MergeTreeRangeReader && other) = default;

private:
    MergeTreeRangeReader(MergeTreeReader & merge_tree_reader, size_t from_mark, size_t to_mark, size_t index_granularity);

    std::reference_wrapper<MergeTreeReader> merge_tree_reader;
    size_t current_mark;
    size_t last_mark;
    size_t read_rows_after_current_mark;
    size_t index_granularity;
    bool is_reading_started;

    friend class MergeTreeReader;
};

}
