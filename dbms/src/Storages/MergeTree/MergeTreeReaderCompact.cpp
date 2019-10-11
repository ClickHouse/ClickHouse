#include <Storages/MergeTree/MergeTreeReaderCompact.h>
#include <Poco/File.h>

namespace DB
{

MergeTreeReaderCompact::MergeTreeReaderCompact(const MergeTreeData::DataPartPtr & data_part_,
    const NamesAndTypesList & columns_, UncompressedCache * uncompressed_cache_, MarkCache * mark_cache_,
    const MarkRanges & mark_ranges_, const ReaderSettings & settings_, const ValueSizeMap & avg_value_size_hints_)
    : IMergeTreeReader(data_part_, columns_
    , uncompressed_cache_, mark_cache_, mark_ranges_
    , settings_, avg_value_size_hints_)
{
}

size_t MergeTreeReaderCompact::readRows(size_t from_mark, bool continue_reading, size_t max_rows_to_read, Block & res)
{
    UNUSED(from_mark);
    UNUSED(continue_reading);
    UNUSED(max_rows_to_read);
    UNUSED(res);

    return 0;
}

void MergeTreeReaderCompact::loadMarks()
{
    const auto & index_granularity_info = data_part->index_granularity_info;
    size_t marks_count = data_part->getMarksCount();
    std::string mrk_path = index_granularity_info.getMarksFilePath(NAME_OF_FILE_WITH_DATA);

    auto load_func = [&]() -> MarkCache::MappedPtr
    {
        size_t file_size = Poco::File(mrk_path).getSize();
        size_t expected_file_size = index_granularity_info.mark_size_in_bytes * marks_count;
        if (expected_file_size != file_size)
            throw Exception(
                "Bad size of marks file '" + mrk_path + "': " + std::to_string(file_size) + ", must be: " + std::to_string(expected_file_size),
                ErrorCodes::CORRUPTED_DATA);

        /// Memory for marks must not be accounted as memory usage for query, because they are stored in shared cache.
        auto temporarily_disable_memory_tracker = getCurrentMemoryTrackerActionLock();

        auto res = std::make_shared<MarksInCompressedFile>(marks_count * columns.size());

        ReadBufferFromFile buffer(mrk_path, file_size);
        size_t i = 0;

        while (!buffer.eof())
        {
            buffer.seek(sizeof(size_t));
            buffer.read(marks.getRowAddress(i), marks.getRowSize());
            ++i;
        }

        if (i * index_granularity_info.mark_size_in_bytes != file_size)
            throw Exception("Cannot read all marks from file " + mrk_path, ErrorCodes::CANNOT_READ_ALL_DATA);

        res->protect();
        return res;
    };

    auto marks_array = IMergeTreeReader::loadMarks(mrk_path, load_func);
    marks = MarksInCompressedFileCompact(marks_array, columns.size());
}

}

