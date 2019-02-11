#include <Common/MemoryTracker.h>
#include <Storages/MergeTree/MergeTreeReaderStream.h>
#include <Poco/File.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int CORRUPTED_DATA;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int ARGUMENT_OUT_OF_BOUND;
}


MergeTreeReaderStream::MergeTreeReaderStream(
        const String & path_prefix_, const String & extension_, size_t marks_count_,
        const MarkRanges & all_mark_ranges,
        MarkCache * mark_cache_, bool save_marks_in_cache_,
        UncompressedCache * uncompressed_cache,
        size_t aio_threshold, size_t max_read_buffer_size,
        const ReadBufferFromFileBase::ProfileCallback & profile_callback, clockid_t clock_type)
        : path_prefix(path_prefix_), extension(extension_), marks_count(marks_count_)
        , mark_cache(mark_cache_), save_marks_in_cache(save_marks_in_cache_)
{
    /// Compute the size of the buffer.
    size_t max_mark_range = 0;

    for (size_t i = 0; i < all_mark_ranges.size(); ++i)
    {
        size_t right = all_mark_ranges[i].end;
        /// NOTE: if we are reading the whole file, then right == marks_count
        /// and we will use max_read_buffer_size for buffer size, thus avoiding the need to load marks.

        /// If the end of range is inside the block, we will need to read it too.
        if (right < marks_count && getMark(right).offset_in_decompressed_block > 0)
        {
            while (right < marks_count
                   && getMark(right).offset_in_compressed_file
                      == getMark(all_mark_ranges[i].end).offset_in_compressed_file)
            {
                ++right;
            }
        }

        /// If there are no marks after the end of range, just use max_read_buffer_size
        if (right >= marks_count
            || (right + 1 == marks_count
                && getMark(right).offset_in_compressed_file
                   == getMark(all_mark_ranges[i].end).offset_in_compressed_file))
        {
            max_mark_range = max_read_buffer_size;
            break;
        }

        max_mark_range = std::max(max_mark_range,
                                  getMark(right).offset_in_compressed_file - getMark(all_mark_ranges[i].begin).offset_in_compressed_file);
    }

    /// Avoid empty buffer. May happen while reading dictionary for DataTypeLowCardinality.
    /// For example: part has single dictionary and all marks point to the same position.
    if (max_mark_range == 0)
        max_mark_range = max_read_buffer_size;

    size_t buffer_size = std::min(max_read_buffer_size, max_mark_range);

    /// Estimate size of the data to be read.
    size_t estimated_size = 0;
    if (aio_threshold > 0)
    {
        for (const auto & mark_range : all_mark_ranges)
        {
            size_t offset_begin = (mark_range.begin > 0)
                                  ? getMark(mark_range.begin).offset_in_compressed_file
                                  : 0;

            size_t offset_end = (mark_range.end < marks_count)
                                ? getMark(mark_range.end).offset_in_compressed_file
                                : Poco::File(path_prefix + extension).getSize();

            if (offset_end > offset_begin)
                estimated_size += offset_end - offset_begin;
        }
    }

    /// Initialize the objects that shall be used to perform read operations.
    if (uncompressed_cache)
    {
        auto buffer = std::make_unique<CachedCompressedReadBuffer>(
                path_prefix + extension, uncompressed_cache, estimated_size, aio_threshold, buffer_size);

        if (profile_callback)
            buffer->setProfileCallback(profile_callback, clock_type);

        cached_buffer = std::move(buffer);
        data_buffer = cached_buffer.get();
    }
    else
    {
        auto buffer = std::make_unique<CompressedReadBufferFromFile>(
                path_prefix + extension, estimated_size, aio_threshold, buffer_size);

        if (profile_callback)
            buffer->setProfileCallback(profile_callback, clock_type);

        non_cached_buffer = std::move(buffer);
        data_buffer = non_cached_buffer.get();
    }
}


const MarkInCompressedFile & MergeTreeReaderStream::getMark(size_t index)
{
    if (!marks)
        loadMarks();
    return (*marks)[index];
}


void MergeTreeReaderStream::loadMarks()
{
    std::string mrk_path = path_prefix + ".mrk";

    auto load = [&]() -> MarkCache::MappedPtr
    {
        /// Memory for marks must not be accounted as memory usage for query, because they are stored in shared cache.
        auto temporarily_disable_memory_tracker = getCurrentMemoryTrackerActionLock();

        size_t file_size = Poco::File(mrk_path).getSize();
        size_t expected_file_size = sizeof(MarkInCompressedFile) * marks_count;
        if (expected_file_size != file_size)
            throw Exception(
                    "bad size of marks file `" + mrk_path + "':" + std::to_string(file_size) + ", must be: " + std::to_string(expected_file_size),
                    ErrorCodes::CORRUPTED_DATA);

        auto res = std::make_shared<MarksInCompressedFile>(marks_count);

        /// Read directly to marks.
        ReadBufferFromFile buffer(mrk_path, file_size, -1, reinterpret_cast<char *>(res->data()));

        if (buffer.eof() || buffer.buffer().size() != file_size)
            throw Exception("Cannot read all marks from file " + mrk_path, ErrorCodes::CANNOT_READ_ALL_DATA);

        return res;
    };

    if (mark_cache)
    {
        auto key = mark_cache->hash(mrk_path);
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
        throw Exception("Failed to load marks: " + mrk_path, ErrorCodes::LOGICAL_ERROR);
}


void MergeTreeReaderStream::seekToMark(size_t index)
{
    MarkInCompressedFile mark = getMark(index);

    try
    {
        if (cached_buffer)
            cached_buffer->seek(mark.offset_in_compressed_file, mark.offset_in_decompressed_block);
        if (non_cached_buffer)
            non_cached_buffer->seek(mark.offset_in_compressed_file, mark.offset_in_decompressed_block);
    }
    catch (Exception & e)
    {
        /// Better diagnostics.
        if (e.code() == ErrorCodes::ARGUMENT_OUT_OF_BOUND)
            e.addMessage("(while seeking to mark " + toString(index)
                         + " of column " + path_prefix + "; offsets are: "
                         + toString(mark.offset_in_compressed_file) + " "
                         + toString(mark.offset_in_decompressed_block) + ")");

        throw;
    }
}


void MergeTreeReaderStream::seekToStart()
{
    try
    {
        if (cached_buffer)
            cached_buffer->seek(0, 0);
        if (non_cached_buffer)
            non_cached_buffer->seek(0, 0);
    }
    catch (Exception & e)
    {
        /// Better diagnostics.
        if (e.code() == ErrorCodes::ARGUMENT_OUT_OF_BOUND)
            e.addMessage("(while seeking to start of column " + path_prefix + ")");

        throw;
    }
}

}
