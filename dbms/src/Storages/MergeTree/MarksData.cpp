#include <Storages/MergeTree/MarksData.h>
#include <Poco/File.h>
#include <Common/MemoryTracker.h>


namespace DB
{
MarksData::MarksData(
    const std::string & path_prefix, const MergeTreeData & storage, size_t marks_count_, bool save_marks_in_cache_, bool lazy_load)
    : path(path_prefix + storage.marks_file_extension)
    , one_mark_bytes_size(storage.one_mark_bytes_size)
    , marks_count(marks_count_ == 0 ? Poco::File(path).getSize() / one_mark_bytes_size : marks_count_)
    , storage_index_granularity(storage.index_granularity)
    , save_marks_in_cache(save_marks_in_cache_)
    , mark_cache(storage.context.getMarkCache())
{
    if (!lazy_load)
        loadMarks();
}

void MarksData::loadMarks()
{
    auto load = [&]() -> MarkCache::MappedPtr {
        /// Memory for marks must not be accounted as memory usage for query, because they are stored in shared cache.
        auto temporarily_disable_memory_tracker = getCurrentMemoryTrackerActionLock();

        size_t file_size = Poco::File(path).getSize();
        size_t expected_file_size = one_mark_bytes_size * marks_count;
        if (expected_file_size != file_size)
            throw Exception(
                "bad size of marks file `" + path + "':" + std::to_string(file_size) + ", must be: " + std::to_string(expected_file_size),
                ErrorCodes::CORRUPTED_DATA);


        auto res = std::make_shared<MarksInCompressedFile>(marks_count);
        /// Read directly to marks.
        ReadBufferFromFile buffer(path, file_size, -1);
        size_t i = 0;
        while (!buffer.eof())
        {
            readIntBinary((*res)[i].offset_in_compressed_file, buffer);
            readIntBinary((*res)[i].offset_in_decompressed_block, buffer);
            if (one_mark_bytes_size == sizeof(MarksInCompressedFile)) /// we have index_granularity in marks file
                readIntBinary((*res)[i].index_granularity, buffer);
            else
                (*res)[i].index_granularity = storage_index_granularity;
            ++i;
        }

        if (i * one_mark_bytes_size != file_size)
            throw Exception("Cannot read all marks from file " + path, ErrorCodes::CANNOT_READ_ALL_DATA);

        return res;
    };

    if (mark_cache)
    {
        auto key = mark_cache->hash(path);
        if (save_marks_in_cache)
        {
            marks = mark_cache->getOrSet(key, load);
        }
        else
        {
            marks = mark_cache->get(key);
            if (!marks)
                marks = load();
        }
    }
    else
        marks = load();

    if (!marks)
        throw Exception("Failed to load marks: " + path, ErrorCodes::LOGICAL_ERROR);
}

const MarkInCompressedFile & MarksData::getMark(size_t index)
{
    if (!marks)
        loadMarks();
    return (*marks)[index];
}
}
