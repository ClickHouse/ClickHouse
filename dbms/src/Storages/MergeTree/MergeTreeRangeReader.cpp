#include <Storages/MergeTree/MergeTreeReader.h>

namespace DB
{

MergeTreeRangeReader::MergeTreeRangeReader(
    MergeTreeReader & merge_tree_reader, size_t from_mark, size_t to_mark, size_t index_granularity)
    : merge_tree_reader(merge_tree_reader), current_mark(from_mark), last_mark(to_mark)
    , read_rows_after_current_mark(0), index_granularity(index_granularity), is_reading_started(false)
{
}


bool MergeTreeRangeReader::read(Block & res, size_t max_rows_to_read)
{
    size_t rows_to_read = (last_mark - current_mark) * index_granularity - read_rows_after_current_mark;
    rows_to_read = std::min(rows_to_read, max_rows_to_read);
    if (rows_to_read == 0)
        return false;

    merge_tree_reader.get().readRange(current_mark, !is_reading_started, rows_to_read, res);

    read_rows_after_current_mark += rows_to_read;
    size_t read_parts = read_rows_after_current_mark / index_granularity;
    current_mark += read_parts;
    read_rows_after_current_mark -= index_granularity * read_parts;

    return current_mark != last_mark;
}

}
