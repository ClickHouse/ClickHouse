#include <Storages/MergeTree/MergeTreeReaderStream.h>
#include <Compression/CachedCompressedReadBuffer.h>

#include <base/getThreadId.h>
#include <utility>


namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int CANNOT_READ_ALL_DATA;
}

MergeTreeReaderStream::MergeTreeReaderStream(
        DataPartStoragePtr data_part_storage_,
        const String & path_prefix_, const String & data_file_extension_, size_t marks_count_,
        const MarkRanges & all_mark_ranges_,
        const MergeTreeReaderSettings & settings_,
        MarkCache * mark_cache_,
        UncompressedCache * uncompressed_cache_, size_t file_size_,
        const MergeTreeIndexGranularityInfo * index_granularity_info_,
        const ReadBufferFromFileBase::ProfileCallback & profile_callback_, clockid_t clock_type_,
        bool is_low_cardinality_dictionary_)
    : settings(settings_)
    , profile_callback(profile_callback_)
    , clock_type(clock_type_)
    , all_mark_ranges(all_mark_ranges_)
    , file_size(file_size_)
    , uncompressed_cache(uncompressed_cache_)
    , data_part_storage(std::move(data_part_storage_))
    , path_prefix(path_prefix_)
    , data_file_extension(data_file_extension_)
    , is_low_cardinality_dictionary(is_low_cardinality_dictionary_)
    , marks_count(marks_count_)
    , mark_cache(mark_cache_)
    , save_marks_in_cache(settings.save_marks_in_cache)
    , index_granularity_info(index_granularity_info_)
    , marks_loader(
        data_part_storage,
        mark_cache,
        index_granularity_info->getMarksFilePath(path_prefix),
        marks_count,
        *index_granularity_info,
        save_marks_in_cache,
        settings.read_settings)
{
}

void MergeTreeReaderStream::init()
{
    if (initialized)
        return;
    initialized = true;
    /// Compute the size of the buffer.
    size_t max_mark_range_bytes = 0;
    size_t sum_mark_range_bytes = 0;

    for (const auto & mark_range : all_mark_ranges)
    {
        size_t left_mark = mark_range.begin;
        size_t right_mark = mark_range.end;
        size_t left_offset = left_mark < marks_count ? marks_loader.getMark(left_mark).offset_in_compressed_file : 0;
        auto mark_range_bytes = getRightOffset(right_mark) - left_offset;

        max_mark_range_bytes = std::max(max_mark_range_bytes, mark_range_bytes);
        sum_mark_range_bytes += mark_range_bytes;
    }

    std::optional<size_t> estimated_sum_mark_range_bytes;
    if (sum_mark_range_bytes)
        estimated_sum_mark_range_bytes.emplace(sum_mark_range_bytes);

    /// Avoid empty buffer. May happen while reading dictionary for DataTypeLowCardinality.
    /// For example: part has single dictionary and all marks point to the same position.
    ReadSettings read_settings = settings.read_settings;
    if (max_mark_range_bytes != 0)
        read_settings = read_settings.adjustBufferSize(max_mark_range_bytes);

    //// Empty buffer does not makes progress.
    if (!read_settings.local_fs_buffer_size || !read_settings.remote_fs_buffer_size)
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Cannot read to empty buffer.");

    /// Initialize the objects that shall be used to perform read operations.
    if (uncompressed_cache)
    {
        auto buffer = std::make_unique<CachedCompressedReadBuffer>(
            std::string(fs::path(data_part_storage->getFullPath()) / (path_prefix + data_file_extension)),
            [this, estimated_sum_mark_range_bytes, read_settings]()
            {
                return data_part_storage->readFile(
                    path_prefix + data_file_extension,
                    read_settings,
                    estimated_sum_mark_range_bytes, std::nullopt);
            },
            uncompressed_cache);

        if (profile_callback)
            buffer->setProfileCallback(profile_callback, clock_type);

        if (!settings.checksum_on_read)
            buffer->disableChecksumming();

        cached_buffer = std::move(buffer);
        data_buffer = cached_buffer.get();
        compressed_data_buffer = cached_buffer.get();
    }
    else
    {
        auto buffer = std::make_unique<CompressedReadBufferFromFile>(
            data_part_storage->readFile(
                path_prefix + data_file_extension,
                read_settings,
                estimated_sum_mark_range_bytes,
                std::nullopt));

        if (profile_callback)
            buffer->setProfileCallback(profile_callback, clock_type);

        if (!settings.checksum_on_read)
            buffer->disableChecksumming();

        non_cached_buffer = std::move(buffer);
        data_buffer = non_cached_buffer.get();
        compressed_data_buffer = non_cached_buffer.get();
    }
}


size_t MergeTreeReaderStream::getRightOffset(size_t right_mark_non_included)
{
    /// NOTE: if we are reading the whole file, then right_mark == marks_count
    /// and we will use max_read_buffer_size for buffer size, thus avoiding the need to load marks.

    /// Special case, can happen in Collapsing/Replacing engines
    if (marks_count == 0)
        return 0;

    assert(right_mark_non_included <= marks_count);

    size_t result_right_offset;
    if (0 < right_mark_non_included && right_mark_non_included < marks_count)
    {
        /// Find the right border of the last mark we need to read.
        /// To do that let's find the upper bound of the offset of the last
        /// included mark.

        /// In LowCardinality dictionary and in values of Sparse columns
        /// several consecutive marks can point to the same offset.
        ///
        /// Example:
        ///  Mark 186, points to [2003111, 0]
        ///  Mark 187, points to [2003111, 0]
        ///  Mark 188, points to [2003111, 0] <--- for example need to read until 188
        ///  Mark 189, points to [2003111, 0] <--- not suitable, because have same offset
        ///  Mark 190, points to [2003111, 0]
        ///  Mark 191, points to [2003111, 0]
        ///  Mark 192, points to [2081424, 0] <--- what we are looking for
        ///  Mark 193, points to [2081424, 0]
        ///  Mark 194, points to [2081424, 0]

        /// Also, in some cases, when one granule is not-atomically written (which is possible at merges)
        /// one granule may require reading of two dictionaries which starts from different marks.
        /// The only correct way is to take offset from at least next different granule from the right one.
        /// So, that's why we have to read one extra granule to the right,
        /// while reading dictionary of LowCardinality.

        /// If right_mark_non_included has non-zero offset in decompressed block, we have to
        /// read its compressed block in a whole, because it may consist data from previous granule.
        ///
        /// For example:
        /// Mark 10: (758287, 0)      <--- right_mark_included
        /// Mark 11: (908457, 53477)  <--- right_mark_non_included
        /// Mark 12: (1064746, 20742) <--- what we are looking for
        /// Mark 13: (2009333, 40123)
        ///
        /// Since mark 11 starts from offset in decompressed block 53477,
        /// it has some data from mark 10 and we have to read
        /// compressed block  [908457; 1064746 in a whole.

        size_t right_mark_included = right_mark_non_included - 1;
        if (is_low_cardinality_dictionary || marks_loader.getMark(right_mark_non_included).offset_in_decompressed_block != 0)
            ++right_mark_included;

        auto indices = collections::range(right_mark_included, marks_count);
        auto it = std::upper_bound(indices.begin(), indices.end(), right_mark_included,
            [&](auto lhs, auto rhs)
            {
                return marks_loader.getMark(lhs).offset_in_compressed_file < marks_loader.getMark(rhs).offset_in_compressed_file;
            });

        if (it != indices.end())
            result_right_offset = marks_loader.getMark(*it).offset_in_compressed_file;
        else
            result_right_offset = file_size;
    }
    else if (right_mark_non_included == 0)
        result_right_offset = marks_loader.getMark(right_mark_non_included).offset_in_compressed_file;
    else
        result_right_offset = file_size;

    return result_right_offset;
}

void MergeTreeReaderStream::seekToMark(size_t index)
{
    init();
    MarkInCompressedFile mark = marks_loader.getMark(index);

    try
    {
        compressed_data_buffer->seek(mark.offset_in_compressed_file, mark.offset_in_decompressed_block);
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
    init();
    try
    {
        compressed_data_buffer->seek(0, 0);
    }
    catch (Exception & e)
    {
        /// Better diagnostics.
        if (e.code() == ErrorCodes::ARGUMENT_OUT_OF_BOUND)
            e.addMessage("(while seeking to start of column " + path_prefix + ")");

        throw;
    }
}


void MergeTreeReaderStream::adjustRightMark(size_t right_mark)
{
    /**
     * Note: this method is called multiple times for the same range of marks -- each time we
     * read from stream, but we must update last_right_offset only if it is bigger than
     * the last one to avoid redundantly cancelling prefetches.
     */
    init();
    auto right_offset = getRightOffset(right_mark);
    if (!right_offset)
    {
        if (last_right_offset && *last_right_offset == 0)
            return;

        last_right_offset = 0; // Zero value means the end of file.
        data_buffer->setReadUntilEnd();
    }
    else
    {
        if (last_right_offset && right_offset <= last_right_offset.value())
            return;

        last_right_offset = right_offset;
        data_buffer->setReadUntilPosition(right_offset);
    }
}

ReadBuffer * MergeTreeReaderStream::getDataBuffer()
{
    init();
    return data_buffer;
}

CompressedReadBufferBase * MergeTreeReaderStream::getCompressedDataBuffer()
{
    init();
    return compressed_data_buffer;
}

}
