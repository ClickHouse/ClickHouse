#include <Storages/MergeTree/MergeTreeReader.h>

namespace DB
{

MergeTreeRangeReader::MergeTreeRangeReader(
    MergeTreeReader & merge_tree_reader, size_t from_mark, size_t to_mark, size_t index_granularity)
    : logger(&Poco::Logger::get("MergeTreeRangeReader"))
    , merge_tree_reader(merge_tree_reader), current_mark(from_mark), last_mark(to_mark)
    , read_rows_after_current_mark(0), index_granularity(index_granularity), seek_to_from_mark(true), is_reading_finished(false)
{
}

size_t MergeTreeRangeReader::skipToNextMark()
{
    auto unread_rows_in_current_part = unreadRowsInCurrentPart();
    seek_to_from_mark = true;
    ++current_mark;
    read_rows_after_current_mark = 0;
    return unread_rows_in_current_part;
}

const MergeTreeRangeReader MergeTreeRangeReader::skipRows(size_t rows) const
{
    MergeTreeRangeReader copy = *this;
    copy.read_rows_after_current_mark += rows;
    size_t read_parts = copy.read_rows_after_current_mark / index_granularity;
    copy.current_mark += read_parts;
    copy.read_rows_after_current_mark -= index_granularity * read_parts;
    return copy;
}

size_t MergeTreeRangeReader::read(Block & res, size_t max_rows_to_read)
{
    size_t rows_to_read = unreadRows();
    rows_to_read = std::min(rows_to_read, max_rows_to_read);
    if (rows_to_read == 0)
        return false;

    auto read_rows = merge_tree_reader.get().readRange(current_mark, seek_to_from_mark, rows_to_read, res);
    /// if no columns to read, consider all rows was read
    if (!read_rows)
        read_rows = rows_to_read;
    seek_to_from_mark = false;

    read_rows_after_current_mark += read_rows;
    size_t read_parts = read_rows_after_current_mark / index_granularity;
    current_mark += read_parts;
    read_rows_after_current_mark -= index_granularity * read_parts;

    if (read_rows < rows_to_read || current_mark == last_mark)
        is_reading_finished = true;

    if (rows_to_read != read_rows && current_mark + 1 != last_mark)
        LOG_ERROR(logger, "!!!!!!!!!!!!!!!!!!!! rows_to_read " << rows_to_read << " read_rows " << read_rows
        << " last_mark = " << last_mark << " current_mark = " << current_mark << " read_rows_after_current_mark  = " << read_rows_after_current_mark);

    return read_rows;
}

MergeTreeRangeReader MergeTreeRangeReader::copyForReader(MergeTreeReader & reader)
{
    MergeTreeRangeReader copy(reader, current_mark, last_mark, index_granularity);
    copy.seek_to_from_mark = seek_to_from_mark;
    copy.read_rows_after_current_mark = read_rows_after_current_mark;
    return copy;
}

}
