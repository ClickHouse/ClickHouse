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

void MergeTreeMarksLoader::loadMarks()
{
    /// Memory for marks must not be accounted as memory usage for query, because they are stored in shared cache.
    auto temporarily_disable_memory_tracker = getCurrentMemoryTrackerActionLock();

    size_t file_size = disk->getFileSize(mrk_path);
    size_t mark_size = index_granularity_info.getMarkSizeInBytes(columns_in_mark);
    size_t expected_file_size = mark_size * marks_count;

    if (expected_file_size != file_size)
        throw Exception(
            "Bad size of marks file '" + fullPath(disk, mrk_path) + "': " + std::to_string(file_size) +
            ", must be: " + std::to_string(expected_file_size),
            ErrorCodes::CORRUPTED_DATA);

    if (!mark_cache)
        /// The cache is off, store this object somewhere in the heap.
        marks_non_cache = std::make_unique<MarksInCompressedFile>(marks_count * columns_in_mark);
    else
    {
        auto key = mark_cache->hash(mrk_path);

        if (save_marks_in_cache)
        {
            const size_t marks_overall_size = marks_count * columns_in_mark;

            constexpr auto size_func = [marks_overall_size] {
                return sizeof(CacheMarksInCompressedFile) * marks_overall_size;
            };

            constexpr auto init_func = [marks_overall_size](void * heap_storage) {
                return MarksInCompressedFile{marks_overall_size, heap_storage};
            };

            /// The cache is active, insert the initial object there and get it reference back.
            marks_cache = mark_cache->getOrSet(key, std::move(size_func), std::move(init_func);
        }
        else
        {
            if (marks_cache = mark_cache->get(key); !marks_cache)
                /// No more other marks, current element not found, store here.
                marks_non_cache = std::make_unique<MarksInCompressedFile>(marks_count * columns_in_mark);

            /// Else new marks will not be stored in the cache, but #marks got right from there.
        }
    }

    if (!marks)
        throw Exception("Failed to load marks: " + mrk_path, ErrorCodes::LOGICAL_ERROR);

    if (!index_granularity_info.is_adaptive)
    {
        /// Read directly to marks.
        auto buffer = disk->readFile(mrk_path, file_size);
        buffer->readStrict(reinterpret_cast<char *>(marks->data()), file_size);

        if (!buffer->eof())
            throw Exception(
                    "Cannot read all marks from file " + mrk_path + ", eof: " + std::to_string(buffer->eof()) +
                    ", buffer size: " + std::to_string(buffer->buffer().size()) +
                    ", file size: " + std::to_string(file_size), ErrorCodes::CANNOT_READ_ALL_DATA);
    }
    else
    {
        auto buffer = disk->readFile(mrk_path, file_size);
        size_t i = 0;

        while (!buffer->eof()) /// propagating while not supported.
        {
            marks->read(*buffer, i * columns_in_mark, columns_in_mark);
            buffer->seek(sizeof(size_t), SEEK_CUR);
            ++i;
        }

        if (i * mark_size != file_size)
            throw Exception("Cannot read all marks from file " + mrk_path, ErrorCodes::CANNOT_READ_ALL_DATA);
    }

    marks->protect();
}
}

