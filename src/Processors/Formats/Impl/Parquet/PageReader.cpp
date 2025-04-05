#include "PageReader.h"

#include <iostream>
#include <arrow/util/int_util_overflow.h>
#include <parquet/thrift_internal.h>
#include <Common/ProfileEvents.h>

namespace ProfileEvents
{
extern const Event ParquetSkipPageNum;
extern const Event ParquetDecompressionTimeMicroseconds;
extern const Event ParquetDeserializingMetaTimeMicroseconds;
}

namespace DB
{
namespace ErrorCodes
{
extern const int PARQUET_EXCEPTION;
}

bool LazyPageReader::hasNext()
{
    Stopwatch time;
    parquet::ThriftDeserializer deserializer(properties);
    uint32_t header_size = 0;
    uint32_t allowed_header_size = DEFAULT_PAGE_HEADER_SIZE;
    while (true)
    {
        if (stream->available() == 0)
        {
            return false;
        }

        // This gets used, then set by DeserializeThriftMsg
        header_size = allowed_header_size;
        try
        {
            // Reset current page header to avoid unclearing the __isset flag.
            current_page_header = parquet::format::PageHeader();
            deserializer.DeserializeMessage(
                reinterpret_cast<const uint8_t *>(stream->position()), &header_size, &current_page_header, nullptr);
            break;
        }
        catch (std::exception & e)
        {
            // Failed to deserialize. Double the allowed page header size and try again
            allowed_header_size *= 2;
            if (allowed_header_size > max_page_header_size)
            {
                throw Exception(ErrorCodes::PARQUET_EXCEPTION, "Deserializing page header failed: {}", e.what());
            }
        }
    }
    stream->seek(header_size, SEEK_CUR);
    increment(ProfileEvents::ParquetDeserializingMetaTimeMicroseconds, time.elapsedMicroseconds());
    return true;
}

template <typename H>
parquet::EncodedStatistics ExtractStatsFromHeader(const H & header)
{
    parquet::EncodedStatistics page_statistics;
    if (!header.__isset.statistics)
    {
        return page_statistics;
    }
    const parquet::format::Statistics & stats = header.statistics;
    // Use the new V2 min-max statistics over the former one if it is filled
    if (stats.__isset.max_value || stats.__isset.min_value)
    {
        if (stats.__isset.max_value)
        {
            page_statistics.set_max(stats.max_value);
        }
        if (stats.__isset.min_value)
        {
            page_statistics.set_min(stats.min_value);
        }
    }
    else if (stats.__isset.max || stats.__isset.min)
    {
        if (stats.__isset.max)
        {
            page_statistics.set_max(stats.max);
        }
        if (stats.__isset.min)
        {
            page_statistics.set_min(stats.min);
        }
    }
    if (stats.__isset.null_count)
    {
        page_statistics.set_null_count(stats.null_count);
    }
    if (stats.__isset.distinct_count)
    {
        page_statistics.set_distinct_count(stats.distinct_count);
    }
    return page_statistics;
}

std::shared_ptr<parquet::Page> LazyPageReader::nextPage()
{
    size_t compressed_len = current_page_header.compressed_page_size;
    size_t uncompressed_len = current_page_header.uncompressed_page_size;
    if (compressed_len > stream->available())
    {
        throw Exception(ErrorCodes::PARQUET_EXCEPTION, "Page was smaller {} than expected {}", stream->available(), compressed_len);
    }
    const parquet::PageType::type page_type = parquet::LoadEnumSafe(&current_page_header.type);

    std::shared_ptr<arrow::Buffer> page_buffer;
    if (page_type == parquet::PageType::DICTIONARY_PAGE)
    {
        page_buffer = decompressIfNeeded(reinterpret_cast<const uint8_t *>(stream->position()), compressed_len, uncompressed_len);
        stream->seek(compressed_len, SEEK_CUR);
        const parquet::format::DictionaryPageHeader & dict_header = current_page_header.dictionary_page_header;
        bool is_sorted = dict_header.__isset.is_sorted ? dict_header.is_sorted : false;
        return std::make_shared<parquet::DictionaryPage>(
            page_buffer, dict_header.num_values, parquet::LoadEnumSafe(&dict_header.encoding), is_sorted);
    }
    else if (page_type == parquet::PageType::DATA_PAGE)
    {
        const parquet::format::DataPageHeader & header = current_page_header.data_page_header;
        parquet::EncodedStatistics data_page_statistics = ExtractStatsFromHeader(header);
        page_buffer = decompressIfNeeded(reinterpret_cast<const uint8_t *>(stream->position()), compressed_len, uncompressed_len);
        stream->seek(compressed_len, SEEK_CUR);
        return std::make_shared<parquet::DataPageV1>(
            page_buffer,
            header.num_values,
            parquet::LoadEnumSafe(&header.encoding),
            parquet::LoadEnumSafe(&header.definition_level_encoding),
            parquet::LoadEnumSafe(&header.repetition_level_encoding),
            uncompressed_len,
            data_page_statistics);
    }
    else if (page_type == parquet::PageType::DATA_PAGE_V2)
    {
        const parquet::format::DataPageHeaderV2 & header = current_page_header.data_page_header_v2;

        // Arrow prior to 3.0.0 set is_compressed to false but still compressed.
        bool is_compressed = (header.__isset.is_compressed ? header.is_compressed : false);

        // Uncompress if needed
        int levels_byte_len;
        if (arrow::internal::AddWithOverflow(header.definition_levels_byte_length, header.repetition_levels_byte_length, &levels_byte_len))
        {
            throw Exception(ErrorCodes::PARQUET_EXCEPTION, "Levels size too large (corrupt file?)");
        }
        // DecompressIfNeeded doesn't take `is_compressed` into account as
        // it's page type-agnostic.
        if (is_compressed)
        {
            page_buffer = decompressIfNeeded(
                reinterpret_cast<const uint8_t *>(stream->position()), compressed_len, uncompressed_len, levels_byte_len);
        }
        parquet::EncodedStatistics data_page_statistics = ExtractStatsFromHeader(header);
        return std::make_shared<parquet::DataPageV2>(
            page_buffer,
            header.num_values,
            header.num_nulls,
            header.num_rows,
            parquet::LoadEnumSafe(&header.encoding),
            header.definition_levels_byte_length,
            header.repetition_levels_byte_length,
            uncompressed_len,
            is_compressed,
            data_page_statistics);
    }
    else
    {
        throw Exception(ErrorCodes::PARQUET_EXCEPTION, "Unsupported page type {}", magic_enum::enum_name(page_type));
    }
}
std::shared_ptr<arrow::Buffer>
LazyPageReader::decompressIfNeeded(const uint8_t * data, size_t compressed_size, size_t uncompressed_size, size_t levels_byte_len)
{
    if (compressed_size < levels_byte_len || uncompressed_size < levels_byte_len)
    {
        throw Exception(ErrorCodes::PARQUET_EXCEPTION, "Invalid page header");
    }
    if (!decompressor)
        return std::make_shared<arrow::Buffer>(data, compressed_size);

    Stopwatch time;
    decompression_buffer.resize(uncompressed_size);

    if (levels_byte_len > 0)
    {
        // First copy the levels as-is
        auto * decompressed = decompression_buffer.data();
        memcpy(decompressed, data, levels_byte_len);
    }

    PARQUET_ASSIGN_OR_THROW(
        auto decompressed_len,
        decompressor->Decompress(
            compressed_size - levels_byte_len,
            data + levels_byte_len,
            uncompressed_size - levels_byte_len,
            reinterpret_cast<uint8_t *>(decompression_buffer.data()) + levels_byte_len))
    increment(ProfileEvents::ParquetDecompressionTimeMicroseconds, time.elapsedMicroseconds());
    if (static_cast<size_t>(decompressed_len) != uncompressed_size - levels_byte_len)
    {
        throw Exception(
            ErrorCodes::PARQUET_EXCEPTION,
            "Page didn't decompress to expected size, expected: {}, but got: {}",
            uncompressed_size - levels_byte_len,
            decompressed_len);
    }

    return std::make_shared<arrow::Buffer>(
        reinterpret_cast<const uint8_t *>(decompression_buffer.data()), static_cast<int64_t>(uncompressed_size));
}
void LazyPageReader::skipNextPage()
{
    size_t compressed_len = current_page_header.compressed_page_size;
    stream->seek(compressed_len, SEEK_CUR);
    ProfileEvents::increment(ProfileEvents::ParquetSkipPageNum, 1);
}
const parquet::format::PageHeader & LazyPageReader::peekNextPageHeader()
{
    return current_page_header;
}

void LazyPageReader::seekFileOffset(size_t offset)
{
    chassert(stream);
    stream->seek(offset - offset_in_file, SEEK_SET);
}
size_t LazyPageReader::getOffsetInFile() const
{
    return stream->offset() + offset_in_file;
}
}
