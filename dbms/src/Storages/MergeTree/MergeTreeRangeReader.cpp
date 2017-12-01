#include <Storages/MergeTree/MergeTreeReader.h>

namespace DB
{

MergeTreeRangeReader::MergeTreeRangeReader(
    MergeTreeReader & merge_tree_reader, size_t from_mark, size_t to_mark, size_t index_granularity)
    : merge_tree_reader(merge_tree_reader), current_mark(from_mark), last_mark(to_mark)
    , index_granularity(index_granularity)
{
}

size_t MergeTreeRangeReader::skipToNextMark()
{
    auto unread_rows_in_current_part = numPendingRowsInCurrentGranule();
    continue_reading = false;
    ++current_mark;
    if (current_mark == last_mark)
        is_reading_finished = true;
    read_rows_after_current_mark = 0;
    return unread_rows_in_current_part;
}

MergeTreeRangeReader MergeTreeRangeReader::getFutureState(size_t rows_to_read) const
{
    MergeTreeRangeReader copy = *this;
    copy.read_rows_after_current_mark += rows_to_read;
    size_t read_parts = copy.read_rows_after_current_mark / index_granularity;
    copy.current_mark += read_parts;
    copy.read_rows_after_current_mark -= index_granularity * read_parts;
    return copy;
}

size_t MergeTreeRangeReader::read(Block & res, size_t max_rows_to_read)
{
    size_t rows_to_read = numPendingRows();
    rows_to_read = std::min(rows_to_read, max_rows_to_read);
    if (rows_to_read == 0)
        throw Exception("Logical error: 0 rows to read.", ErrorCodes::LOGICAL_ERROR);

    auto read_rows = merge_tree_reader.get().readRows(current_mark, continue_reading, rows_to_read, res);

    if (read_rows && read_rows < rows_to_read)
        is_reading_finished = true;

    if (!read_rows)
        read_rows = rows_to_read;

    continue_reading = true;

    read_rows_after_current_mark += read_rows;
    size_t read_parts = read_rows_after_current_mark / index_granularity;
    current_mark += read_parts;
    read_rows_after_current_mark -= index_granularity * read_parts;

    if (current_mark == last_mark)
        is_reading_finished = true;

    return read_rows;
}

MergeTreeRangeReader MergeTreeRangeReader::copyForReader(MergeTreeReader & reader)
{
    MergeTreeRangeReader copy(reader, current_mark, last_mark, index_granularity);
    copy.continue_reading = continue_reading;
    copy.read_rows_after_current_mark = read_rows_after_current_mark;
    return copy;
}

}
