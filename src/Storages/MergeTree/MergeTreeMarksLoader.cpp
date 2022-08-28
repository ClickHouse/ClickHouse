#include <Storages/MergeTree/MergeTreeMarksLoader.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <IO/ReadBufferFromFile.h>
#include <Compression/CompressedReadBufferFromFile.h>

#include <utility>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
    extern const int CORRUPTED_DATA;
    extern const int LOGICAL_ERROR;
}

MergeTreeMarksLoader::MergeTreeMarksLoader(
    DataPartStoragePtr data_part_storage_,
    MarkCache * mark_cache_,
    const String & mrk_path_,
    size_t marks_count_,
    const MergeTreeIndexGranularityInfo & index_granularity_info_,
    bool save_marks_in_cache_,
    const ReadSettings & read_settings_,
    size_t columns_in_mark_)
    : data_part_storage(std::move(data_part_storage_))
    , mark_cache(mark_cache_)
    , mrk_path(mrk_path_)
    , marks_count(marks_count_)
    , index_granularity_info(index_granularity_info_)
    , save_marks_in_cache(save_marks_in_cache_)
    , columns_in_mark(columns_in_mark_)
    , read_settings(read_settings_)
{
}


const MarkInCompressedFile & MergeTreeMarksLoader::getMark(size_t row_index, size_t column_index)
{
    if (!marks)
        loadMarks();

#ifndef NDEBUG
    if (column_index >= columns_in_mark)
        throw Exception("Column index: " + toString(column_index)
            + " is out of range [0, " + toString(columns_in_mark) + ")", ErrorCodes::LOGICAL_ERROR);
#endif

    return (*marks)[row_index * columns_in_mark + column_index];
}


MarkCache::MappedPtr MergeTreeMarksLoader::loadMarksImpl()
{
    /// Memory for marks must not be accounted as memory usage for query, because they are stored in shared cache.
    MemoryTrackerBlockerInThread temporarily_disable_memory_tracker;

    size_t file_size = data_part_storage->getFileSize(mrk_path);
    size_t mark_size = index_granularity_info.getMarkSizeInBytes(columns_in_mark);
    size_t expected_uncompressed_size = mark_size * marks_count;

    auto res = std::make_shared<MarksInCompressedFile>(marks_count * columns_in_mark);

    if (!index_granularity_info.compress_marks && expected_uncompressed_size != file_size)
        throw Exception(
            ErrorCodes::CORRUPTED_DATA,
            "Bad size of marks file '{}': {}, must be: {}",
            std::string(fs::path(data_part_storage->getFullPath()) / mrk_path),
            std::to_string(file_size), std::to_string(expected_uncompressed_size));

    auto buffer = data_part_storage->readFile(mrk_path, read_settings.adjustBufferSize(file_size), file_size, std::nullopt);
    std::unique_ptr<ReadBuffer> reader;
    if (index_granularity_info.compress_marks)
        reader = std::make_unique<CompressedReadBufferFromFile>(std::move(buffer));
    else
        reader = std::move(buffer);

    if (!index_granularity_info.is_adaptive)
    {
        /// Read directly to marks.
        reader->readStrict(reinterpret_cast<char *>(res->data()), expected_uncompressed_size);

        if (!reader->eof())
            throw Exception("Cannot read all marks from file " + mrk_path + ", eof: " + std::to_string(reader->eof())
                            + ", buffer size: " + std::to_string(reader->buffer().size()) + ", file size: " + std::to_string(file_size), ErrorCodes::CANNOT_READ_ALL_DATA);
    }
    else
    {
        size_t i = 0;
        size_t granularity;
        while (!reader->eof())
        {
            res->read(*reader, i * columns_in_mark, columns_in_mark);
            readIntBinary(granularity, *reader);
            ++i;
        }

        if (i * mark_size != expected_uncompressed_size)
            throw Exception("Cannot read all marks from file " + mrk_path, ErrorCodes::CANNOT_READ_ALL_DATA);
    }

    res->protect();
    return res;
}


void MergeTreeMarksLoader::loadMarks()
{
    if (mark_cache)
    {
        auto key = mark_cache->hash(fs::path(data_part_storage->getFullPath()) / mrk_path);
        if (save_marks_in_cache)
        {
            auto callback = [this]{ return loadMarksImpl(); };
            marks = mark_cache->getOrSet(key, callback);
        }
        else
        {
            marks = mark_cache->get(key);
            if (!marks)
                marks = loadMarksImpl();
        }
    }
    else
        marks = loadMarksImpl();

    if (!marks)
        throw Exception("Failed to load marks: " + std::string(fs::path(data_part_storage->getFullPath()) / mrk_path), ErrorCodes::LOGICAL_ERROR);
}

}
