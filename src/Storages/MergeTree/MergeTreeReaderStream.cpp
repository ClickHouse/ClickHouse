#include <Storages/MergeTree/MergeTreeReaderStream.h>
#include <Compression/CachedCompressedReadBuffer.h>

#include <base/getThreadId.h>
#include <base/range.h>
#include <utility>


namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int LOGICAL_ERROR;
}

MergeTreeReaderStream::MergeTreeReaderStream(
    DataPartStoragePtr data_part_storage_,
    const String & path_prefix_,
    const String & data_file_extension_,
    size_t marks_count_,
    const MarkRanges & all_mark_ranges_,
    const MergeTreeReaderSettings & settings_,
    UncompressedCache * uncompressed_cache_,
    size_t file_size_,
    MergeTreeMarksLoaderPtr marks_loader_,
    const ReadBufferFromFileBase::ProfileCallback & profile_callback_,
    clockid_t clock_type_)
    : profile_callback(profile_callback_)
    , clock_type(clock_type_)
    , all_mark_ranges(all_mark_ranges_)
    , data_part_storage(std::move(data_part_storage_))
    , path_prefix(path_prefix_)
    , data_file_extension(data_file_extension_)
    , uncompressed_cache(uncompressed_cache_)
    , settings(settings_)
    , marks_count(marks_count_)
    , file_size(file_size_)
    , marks_loader(std::move(marks_loader_))
{
}

void MergeTreeReaderStream::loadMarks()
{
    if (!marks_getter)
        marks_getter = marks_loader->loadMarks();
}

void MergeTreeReaderStream::init()
{
    if (initialized)
        return;

    /// Compute the size of the buffer.
    auto [max_mark_range_bytes, sum_mark_range_bytes] = estimateMarkRangeBytes(all_mark_ranges);

    std::optional<size_t> estimated_sum_mark_range_bytes;
    if (sum_mark_range_bytes)
        estimated_sum_mark_range_bytes.emplace(sum_mark_range_bytes);

    /// Avoid empty buffer. May happen while reading dictionary for DataTypeLowCardinality.
    /// For example: part has single dictionary and all marks point to the same position.
    ReadSettings read_settings = settings.read_settings;
    if (settings.adjust_read_buffer_size && max_mark_range_bytes != 0)
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
            uncompressed_cache,
            settings.allow_different_codecs);

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
                std::nullopt), settings.allow_different_codecs);

        if (profile_callback)
            buffer->setProfileCallback(profile_callback, clock_type);

        if (!settings.checksum_on_read)
            buffer->disableChecksumming();

        non_cached_buffer = std::move(buffer);
        data_buffer = non_cached_buffer.get();
        compressed_data_buffer = non_cached_buffer.get();
    }

    initialized = true;
}

void MergeTreeReaderStream::seekToMarkAndColumn(size_t row_index, size_t column_position)
{
    init();
    loadMarks();

    const auto & mark = marks_getter->getMark(row_index, column_position);

    try
    {
        compressed_data_buffer->seek(mark.offset_in_compressed_file, mark.offset_in_decompressed_block);
    }
    catch (Exception & e)
    {
        /// Better diagnostics.
        if (e.code() == ErrorCodes::ARGUMENT_OUT_OF_BOUND)
            e.addMessage("(while seeking to mark " + toString(row_index)
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

size_t MergeTreeReaderStreamSingleColumn::getRightOffset(size_t right_mark)
{
    /// NOTE: if we are reading the whole file, then right_mark == marks_count
    /// and we will use max_read_buffer_size for buffer size, thus avoiding the need to load marks.

    /// Special case, can happen in Collapsing/Replacing engines
    if (marks_count == 0)
        return 0;

    chassert(right_mark <= marks_count);
    loadMarks();

    if (right_mark == 0)
        return marks_getter->getMark(right_mark, 0).offset_in_compressed_file;

    if (right_mark == marks_count)
        return file_size;

    /// Find the right border of the last mark we need to read.
    /// To do that let's find the upper bound of the offset of the last
    /// included mark.

    if (settings.is_low_cardinality_dictionary)
    {
        /// In LowCardinality dictionary several consecutive marks can point to the same offset.
        ///
        /// Also, in some cases, when one granule is not-atomically written (which is possible at merges)
        /// one granule may require reading of two dictionaries which starts from different marks.
        /// The only correct way is to take offset from at least next different granule from the right one.
        /// So, that's why we have to read one extra granule to the right,
        /// while reading dictionary of LowCardinality.
        ///
        /// Example:
        /// Mark 0, points to [0, 8]
        /// Mark 1, points to [0, 8]
        /// Mark 2, points to [0, 8]
        /// Mark 3, points to [0, 8]
        /// Mark 4, points to [42336, 2255]
        /// Mark 5, points to [42336, 2255]  <--- for example need to read until 5
        /// Mark 6, points to [42336, 2255]  <--- not suitable, because have same offset
        /// Mark 7, points to [84995, 7738]  <--- next different mark
        /// Mark 8, points to [84995, 7738]
        /// Mark 9, points to [126531, 8637] <--- what we are looking for

        auto indices = collections::range(right_mark, marks_count);
        auto next_different_mark = [&](auto lhs, auto rhs)
        {
            return marks_getter->getMark(lhs, 0).asTuple() < marks_getter->getMark(rhs, 0).asTuple();
        };

        auto it = std::upper_bound(indices.begin(), indices.end(), right_mark, std::move(next_different_mark));
        if (it == indices.end())
            return file_size;

        right_mark = *it;
    }

    /// This is a good scenario. The compressed block is finished within the right mark,
    /// and previous mark was different.
    if (marks_getter->getMark(right_mark, 0).offset_in_decompressed_block == 0
        && marks_getter->getMark(right_mark, 0) != marks_getter->getMark(right_mark - 1, 0))
        return marks_getter->getMark(right_mark, 0).offset_in_compressed_file;

    /// If right_mark has non-zero offset in decompressed block, we have to
    /// read its compressed block in a whole, because it may consist of data from previous granule.
    ///
    /// For example:
    /// Mark 6, points to [42336, 2255]
    /// Mark 7, points to [84995, 7738]  <--- right_mark
    /// Mark 8, points to [84995, 7738]
    /// Mark 9, points to [126531, 8637] <--- what we are looking for
    ///
    /// Since mark 7 starts from offset in decompressed block 7738,
    /// it has some data from mark 6 and we have to read
    /// compressed block  [84995; 126531 in a whole.

    auto indices = collections::range(right_mark, marks_count);
    auto next_different_compressed_offset = [&](auto lhs, auto rhs)
    {
        return marks_getter->getMark(lhs, 0).offset_in_compressed_file < marks_getter->getMark(rhs, 0).offset_in_compressed_file;
    };

    auto it = std::upper_bound(indices.begin(), indices.end(), right_mark, std::move(next_different_compressed_offset));
    if (it != indices.end())
        return marks_getter->getMark(*it, 0).offset_in_compressed_file;

    return file_size;
}

std::pair<size_t, size_t> MergeTreeReaderStreamSingleColumn::estimateMarkRangeBytes(const MarkRanges & mark_ranges)
{
    loadMarks();

    size_t max_range_bytes = 0;
    size_t sum_range_bytes = 0;

    for (const auto & mark_range : mark_ranges)
    {
        size_t left_mark = mark_range.begin;
        size_t right_mark = mark_range.end;
        size_t left_offset = left_mark < marks_count ? marks_getter->getMark(left_mark, 0).offset_in_compressed_file : 0;
        auto mark_range_bytes = getRightOffset(right_mark) - left_offset;

        max_range_bytes = std::max(max_range_bytes, mark_range_bytes);
        sum_range_bytes += mark_range_bytes;
    }

    return {max_range_bytes, sum_range_bytes};
}

size_t MergeTreeReaderStreamSingleColumnWholePart::getRightOffset(size_t right_mark)
{
    if (right_mark != marks_count)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Expected one right mark: {}, got: {}",
            marks_count, right_mark);
    }
    return file_size;
}

std::pair<size_t, size_t> MergeTreeReaderStreamSingleColumnWholePart::estimateMarkRangeBytes(const MarkRanges & mark_ranges)
{
    if (!mark_ranges.isOneRangeForWholePart(marks_count))
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Expected one mark range that covers the whole part, got: {}",
            mark_ranges.describe());
    }
    return {file_size, file_size};
}

void MergeTreeReaderStreamSingleColumnWholePart::seekToMark(size_t)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "MergeTreeReaderStreamSingleColumnWholePart cannot seek to marks");
}

size_t MergeTreeReaderStreamMultipleColumns::getRightOffsetOneColumn(size_t right_mark_non_included, size_t column_position)
{
    /// NOTE: if we are reading the whole file, then right_mark == marks_count
    /// and we will use max_read_buffer_size for buffer size, thus avoiding the need to load marks.

    /// Special case, can happen in Collapsing/Replacing engines
    if (marks_count == 0)
        return 0;

    chassert(right_mark_non_included <= marks_count);
    loadMarks();

    if (right_mark_non_included == 0)
        return marks_getter->getMark(right_mark_non_included, column_position).offset_in_compressed_file;

    size_t right_mark_included = right_mark_non_included - 1;
    if (right_mark_non_included != marks_count
        && marks_getter->getMark(right_mark_non_included, column_position).offset_in_decompressed_block != 0)
         ++right_mark_included;

    /// The right bound for case, where there is no smaller suitable mark
    /// is the start of the next stripe (in which the next column is written)
    /// because each stripe always start from a new compressed block.
    const auto & right_mark_in_file = marks_getter->getMark(right_mark_included, column_position);
    auto next_stripe_right_mark_in_file = getStartOfNextStripeMark(right_mark_included, column_position);

    /// Try to find suitable right mark in current stripe.
    for (size_t mark = right_mark_included + 1; mark < marks_count; ++mark)
    {
        const auto & current_mark = marks_getter->getMark(mark, column_position);
        /// We found first mark that starts from the new compressed block.
        if (current_mark.offset_in_compressed_file > right_mark_in_file.offset_in_compressed_file)
        {
            /// If it is in current stripe return it to reduce amount of read data.
            if (current_mark < next_stripe_right_mark_in_file)
                return current_mark.offset_in_compressed_file;

            /// Otherwise return start of new stripe as an upper bound.
            break;
        }
    }

    return next_stripe_right_mark_in_file.offset_in_compressed_file;
}

std::pair<size_t, size_t>
MergeTreeReaderStreamMultipleColumns::estimateMarkRangeBytesOneColumn(const MarkRanges & mark_ranges, size_t column_position)
{
    loadMarks();

    /// As a maximal range we return the maximal size of a whole stripe.
    size_t max_range_bytes = 0;
    size_t sum_range_bytes = 0;

    for (const auto & mark_range : mark_ranges)
    {
        auto start_of_stripe_mark = marks_getter->getMark(mark_range.begin, column_position);
        auto start_of_next_stripe_mark = getStartOfNextStripeMark(mark_range.begin, column_position);

        for (size_t mark = mark_range.begin; mark < mark_range.end; ++mark)
        {
            const auto & current_mark = marks_getter->getMark(mark, column_position);

            /// We found a start of new stripe, now update values.
            if (current_mark > start_of_next_stripe_mark)
            {
                auto current_range_bytes = getRightOffsetOneColumn(mark, column_position) - start_of_stripe_mark.offset_in_compressed_file;

                max_range_bytes = std::max(max_range_bytes, current_range_bytes);
                sum_range_bytes += current_range_bytes;

                start_of_stripe_mark = current_mark;
                start_of_next_stripe_mark = getStartOfNextStripeMark(mark, column_position);
            }
        }

        auto current_range_bytes = getRightOffsetOneColumn(mark_range.end, column_position) - start_of_stripe_mark.offset_in_compressed_file;

        max_range_bytes = std::max(max_range_bytes, current_range_bytes);
        sum_range_bytes += current_range_bytes;
    }

    return {max_range_bytes, sum_range_bytes};
}

MarkInCompressedFile MergeTreeReaderStreamMultipleColumns::getStartOfNextStripeMark(size_t row_index, size_t column_position)
{
    loadMarks();
    const auto & current_mark = marks_getter->getMark(row_index, column_position);

    if (marks_getter->getNumColumns() == 1)
        return MarkInCompressedFile{file_size, 0};

    if (column_position + 1 == marks_getter->getNumColumns())
    {
        /**
         * In case of the last column (c3), we have the following picture:
         *                   c1 c2 c3
         *                   x  x  x
         * (row_index, 0) -> o  x  o <- (row_index, column_position)
         *                   x  x  x
         *                   ------- <- start of new stripe
         *    what we are -> o  x  x
         *    looking for    x  x  x
         *                   x  x  x
         *                   -------
         * So, we need to iterate forward.
         */
        size_t mark_index = row_index + 1;
        while (mark_index < marks_count && marks_getter->getMark(mark_index, 0) <= current_mark)
            ++mark_index;

        return mark_index == marks_count
            ? MarkInCompressedFile{file_size, 0}
            : marks_getter->getMark(mark_index, 0);
    }

    /**
    * Otherwise, we have the following picture:
    *                c1 c2 c3
    *                x  x  o <- what we are looking for
    * (row, column) --> o  o <- (row, column + 1)
    *                x  x  x
    *                ------- <- start of new stripe
    * So, we need to iterate backward.
    */

    ssize_t mark_index = row_index;
    while (mark_index >= 0 && marks_getter->getMark(mark_index, column_position + 1) >= current_mark)
        --mark_index;

    return marks_getter->getMark(mark_index + 1, column_position + 1);
}

size_t MergeTreeReaderStreamOneOfMultipleColumns::getRightOffset(size_t right_mark_non_included)
{
    return getRightOffsetOneColumn(right_mark_non_included, column_position);
}

std::pair<size_t, size_t> MergeTreeReaderStreamOneOfMultipleColumns::estimateMarkRangeBytes(const MarkRanges & mark_ranges)
{
    return estimateMarkRangeBytesOneColumn(mark_ranges, column_position);
}

size_t MergeTreeReaderStreamAllOfMultipleColumns::getRightOffset(size_t right_mark_non_included)
{
    return getRightOffsetOneColumn(right_mark_non_included, marks_loader->getNumColumns() - 1);
}

std::pair<size_t, size_t> MergeTreeReaderStreamAllOfMultipleColumns::estimateMarkRangeBytes(const MarkRanges & mark_ranges)
{
    size_t max_range_bytes = 0;
    size_t sum_range_bytes = 0;

    for (size_t i = 0; i < marks_loader->getNumColumns(); ++i)
    {
        auto [current_max, current_sum] = estimateMarkRangeBytesOneColumn(mark_ranges, i);

        max_range_bytes = std::max(max_range_bytes, current_max);
        sum_range_bytes += current_sum;
    }

    return {max_range_bytes, sum_range_bytes};
}

}
