#include <Storages/MergeTree/MergeTreeMarksLoader.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <IO/ReadBufferFromFile.h>
#include <Poco/File.h>

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
    DiskPtr disk_,
    MarkCache * mark_cache_,
    const String & mrk_path_,
    size_t marks_count_,
    const MergeTreeIndexGranularityInfo & index_granularity_info_,
    bool save_marks_in_cache_,
    size_t columns_in_mark_)
    : disk(std::move(disk_))
    , mark_cache(mark_cache_)
    , mrk_path(mrk_path_)
    , marks_count(marks_count_)
    , index_granularity_info(index_granularity_info_)
    , save_marks_in_cache(save_marks_in_cache_)
    , columns_in_mark(columns_in_mark_) {}

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
    MemoryTracker::BlockerInThread temporarily_disable_memory_tracker;

    size_t file_size = disk->getFileSize(mrk_path);
    size_t mark_size = index_granularity_info.getMarkSizeInBytes(columns_in_mark);
    size_t expected_file_size = mark_size * marks_count;

    if (expected_file_size != file_size)
        throw Exception(
            "Bad size of marks file '" + fullPath(disk, mrk_path) + "': " + std::to_string(file_size) + ", must be: " + std::to_string(expected_file_size),
            ErrorCodes::CORRUPTED_DATA);

    auto res = std::make_shared<MarksInCompressedFile>(marks_count * columns_in_mark);

    if (!index_granularity_info.is_adaptive)
    {
        /// Read directly to marks.
        auto buffer = disk->readFile(mrk_path, file_size);
        buffer->readStrict(reinterpret_cast<char *>(res->data()), file_size);

        if (!buffer->eof())
            throw Exception("Cannot read all marks from file " + mrk_path + ", eof: " + std::to_string(buffer->eof())
            + ", buffer size: " + std::to_string(buffer->buffer().size()) + ", file size: " + std::to_string(file_size), ErrorCodes::CANNOT_READ_ALL_DATA);
    }
    else
    {
        auto buffer = disk->readFile(mrk_path, file_size);
        size_t i = 0;
        while (!buffer->eof())
        {
            res->read(*buffer, i * columns_in_mark, columns_in_mark);
            buffer->seek(sizeof(size_t), SEEK_CUR);
            ++i;
        }

        if (i * mark_size != file_size)
            throw Exception("Cannot read all marks from file " + mrk_path, ErrorCodes::CANNOT_READ_ALL_DATA);
    }
    res->protect();
    return res;
}

void MergeTreeMarksLoader::loadMarks()
{
    if (mark_cache)
    {
        auto key = mark_cache->hash(mrk_path);
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
        throw Exception("Failed to load marks: " + mrk_path, ErrorCodes::LOGICAL_ERROR);
}

}
