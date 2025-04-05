#include "DecoderHelper.h"

#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Processors/Formats/Impl/Parquet/ParquetColumnReaderFactory.h>
#include <Processors/Formats/Impl/Parquet/ParquetReader.h>
#include <Common/assert_cast.h>

namespace DB
{
static void appendString(ColumnString::Chars & chars, IColumn::Offsets & offsets, const String & value)
{
    if (!value.empty())
    {
        auto chars_cursor = chars.size();
        chars.resize(chars_cursor + value.size() + 1);
        memcpySmallAllowReadWriteOverflow15(&chars[chars_cursor], value.data(), value.size());
        chars.back() = 0;
    }
    else
        chars.push_back(0);
    offsets.push_back(chars.size());
}

void DictDecoder::decodeStringSpace(
    std::vector<String> & dict,
    ColumnString::Chars & chars,
    IColumn::Offsets & string_offsets,
    const OptionalRowSet & row_set,
    PaddedPODArray<UInt8> & null_map,
    const size_t rows_to_read) const
{
    size_t rows_read = 0;
    size_t count = 0;
    if (row_set.has_value())
    {
        const auto & sets = row_set.value();
        while (rows_read < rows_to_read)
        {
            if (sets.get(rows_read))
            {
                if (null_map[rows_read])
                {
                    chars.push_back(0);
                    string_offsets.push_back(chars.size());
                }
                else
                {
                    const String & value = dict.at(idx_buffer[count++]);
                    appendString(chars, string_offsets, value);
                }
            }
            else if (!null_map[rows_read])
                count++;
            rows_read++;
        }
    }
    else
    {
        while (rows_read < rows_to_read)
        {
            if (null_map[rows_read])
            {
                chars.push_back(0);
                string_offsets.push_back(chars.size());
            }
            else
            {
                const String & value = dict.at(idx_buffer[count++]);
                appendString(chars, string_offsets, value);
            }
            rows_read++;
        }
    }
    chassert(count == idx_buffer.size());
    this->offsets.consume(rows_to_read);
    idx_buffer.resize(0);
}

void DictDecoder::decodeString(
    std::vector<String> & dict,
    ColumnString::Chars & chars,
    IColumn::Offsets & string_offsets,
    const OptionalRowSet & row_set,
    const size_t rows_to_read) const
{
    if (row_set.has_value())
    {
        for (size_t i = 0; i < rows_to_read; i++)
        {
            if (row_set.value().get(i))
            {
                const String & value = dict.at(idx_buffer[i]);
                appendString(chars, string_offsets, value);
            }
        }
    }
    else
    {
        for (size_t i = 0; i < rows_to_read; i++)
        {
            const String & value = dict.at(idx_buffer[i]);
            appendString(chars, string_offsets, value);
        }
    }
    idx_buffer.resize(0);
    this->offsets.consume(rows_to_read);
}

void DictDecoder::decodeFixedString(
    PaddedPODArray<String> & dict, ColumnFixedString::Chars & chars, const OptionalRowSet & row_set, size_t rows_to_read)
{
    const bool has_set = row_set.has_value();
    chars.reserve(chars.size() + rows_to_read * dict[0].size());
    for (size_t i = 0; i < rows_to_read; i++)
    {
        if (!has_set || row_set.value().get(i))
        {
            const String & value = dict[idx_buffer[i]];
            chars.insert_assume_reserved(value.data(), value.data() + value.size());
        }
    }
    idx_buffer.resize(0);
    this->offsets.consume(rows_to_read);
}

template <class DictValueType>
void DictDecoder::decodeFixedValue(
    PaddedPODArray<DictValueType> & dict, PaddedPODArray<DictValueType> & data, const OptionalRowSet & row_set, size_t rows_to_read)
{
    if (row_set.has_value())
    {
        const auto & sets = row_set.value();
        FilterHelper::filterDictFixedData(dict, data, idx_buffer, sets, rows_to_read);
    }
    else
    {
        FilterHelper::gatherDictFixedValue(dict, data, idx_buffer, rows_to_read);
    }
    idx_buffer.resize(0);
    this->offsets.consume(rows_to_read);
}

void DictDecoder::decodeFixedStringSpace(
    PaddedPODArray<String> & dict,
    ColumnFixedString::Chars & chars,
    const OptionalRowSet & row_set,
    PaddedPODArray<UInt8> & null_map,
    const size_t rows_to_read,
    const size_t element_size) const
{
    size_t rows_read = 0;
    size_t count = 0;
    chars.reserve(chars.size() + rows_to_read * element_size);
    const bool has_set = row_set.has_value();
    while (rows_read < rows_to_read)
    {
        if (!has_set || row_set.value().get(rows_read))
        {
            if (null_map[rows_read])
                chars.resize(chars.size() + element_size);
            else
            {
                auto & value = dict[idx_buffer[count++]];
                chars.insert_assume_reserved(value.data(), value.data() + element_size);
            }
        }
        else if (!null_map[rows_read])
            count++;
        rows_read++;
    }
    this->offsets.consume(rows_to_read);
    idx_buffer.resize(0);
}

void PlainDecoder::decodeString(
    ColumnString::Chars & chars, IColumn::Offsets & string_offsets, const OptionalRowSet & row_set, size_t rows_to_read) const
{
    size_t offset = 0;
    if (row_set.has_value())
    {
        const auto & sets = row_set.value();
        for (size_t i = 0; i < rows_to_read; i++)
        {
            auto len = loadLength(page_data.buffer + offset);
            offset += 4;
            if (sets.get(i))
            {
                if (len)
                    chars.insert_assume_reserved(page_data.buffer + offset, page_data.buffer + offset + len);
                chars.push_back(0);
                string_offsets.push_back(chars.size());
                offset += len;
            }
            else
            {
                offset += len;
            }
        }
    }
    else
    {
        for (size_t i = 0; i < rows_to_read; i++)
        {
            auto len = loadLength(page_data.buffer + offset);
            offset += 4;
            if (len)
                chars.insert_assume_reserved(page_data.buffer + offset, page_data.buffer + offset + len);
            chars.push_back(0);
            string_offsets.push_back(chars.size());
            offset += len;
        }
    }
    page_data.checkAndConsume(offset);
    this->offsets.consume(rows_to_read);
}

void PlainDecoder::decodeStringSpace(
    ColumnString::Chars & chars,
    IColumn::Offsets & string_offsets,
    const OptionalRowSet & row_set,
    PaddedPODArray<UInt8, 4096> & null_map,
    size_t rows_to_read) const
{
    size_t offset = 0;
    if (row_set.has_value())
    {
        const auto & sets = row_set.value();
        for (size_t i = 0; i < rows_to_read; i++)
        {
            if (null_map[i])
            {
                if (sets.get(i))
                {
                    // null string
                    chars.push_back(0);
                    string_offsets.push_back(chars.size());
                }
                continue;
            }
            auto len = loadLength(page_data.buffer + offset);
            offset += 4;
            if (sets.get(i))
            {
                if (len)
                    chars.insert_assume_reserved(page_data.buffer + offset, page_data.buffer + offset + len);
                chars.push_back(0);
                string_offsets.push_back(chars.size());
                offset += len;
            }
            else
            {
                offset += len;
            }
        }
    }
    else
    {
        for (size_t i = 0; i < rows_to_read; i++)
        {
            if (null_map[i])
            {
                chars.push_back(0);
                string_offsets.push_back(chars.size());
                continue;
            }
            auto len = loadLength(page_data.buffer + offset);
            offset += 4;
            if (len)
                chars.insert_assume_reserved(page_data.buffer + offset, page_data.buffer + offset + len);
            chars.push_back(0);
            string_offsets.push_back(chars.size());
            offset += len;
        }
    }
    page_data.checkAndConsume(offset);
    this->offsets.consume(rows_to_read);
}
size_t PlainDecoder::calculateStringTotalSize(const ParquetData & data, const OptionalRowSet & row_set, const size_t rows_to_read)
{
    size_t offset = 0;
    size_t total_size = 0;
    for (size_t i = 0; i < rows_to_read; i++)
    {
        addOneString(false, data, offset, row_set, i, total_size);
    }
    return total_size;
}
size_t DB::PlainDecoder::calculateStringTotalSizeSpace(
    const ParquetData & data, const DB::OptionalRowSet & row_set, DB::PaddedPODArray<UInt8> & null_map, const size_t rows_to_read)
{
    size_t offset = 0;
    size_t total_size = 0;
    for (size_t i = 0; i < rows_to_read; i++)
    {
        addOneString(null_map[i], data, offset, row_set, i, total_size);
    }
    return total_size;
}

void PlainDecoder::decodeFixedString(ColumnFixedString::Chars & data, const OptionalRowSet & row_set, size_t rows_to_read, size_t n) const
{
    data.reserve(data.size() + (n * rows_to_read));
    for (size_t i = 0; i < rows_to_read; i++)
    {
        if (!row_set || row_set.value().get(i))
        {
            page_data.checkSize(n);
            data.insert_assume_reserved(page_data.buffer, page_data.buffer + n);
            page_data.consume(n);
        }
        else
            page_data.checkAndConsume(n);
    }
    offsets.consume(rows_to_read);
}

void PlainDecoder::decodeFixedStringSpace(
    ColumnFixedString::Chars & data, const OptionalRowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t rows_to_read, size_t n) const
{
    data.reserve(data.size() + (n * rows_to_read));
    for (size_t i = 0; i < rows_to_read; i++)
    {
        if (!row_set || row_set.value().get(i))
        {
            if (null_map[i])
            {
                data.resize(data.size() + n);
            }
            else
            {
                page_data.checkSize(n);
                data.insert_assume_reserved(page_data.buffer, page_data.buffer + n);
                page_data.consume(n);
            }
        }
        else
        {
            if (!null_map[i])
                page_data.checkAndConsume(n);
        }
    }
    offsets.consume(rows_to_read);
}


void PlainDecoder::decodeBooleanSpace(
    PaddedPODArray<UInt8> & data,
    PaddedPODArray<UInt8> & src,
    const OptionalRowSet & row_set,
    PaddedPODArray<UInt8> & null_map,
    size_t rows_to_read) const
{
    decodeFixedValueSpaceInternal(data, src.data(), row_set, null_map, rows_to_read);
    offsets.consume(rows_to_read);
}

void PlainDecoder::addOneString(
    bool null, const ParquetData & data, size_t & offset, const OptionalRowSet & row_set, size_t row, size_t & total_size)
{
    if (row_set.has_value())
    {
        const auto & sets = row_set.value();
        if (null)
        {
            if (sets.get(row))
                total_size++;
            return;
        }
        data.checkSize(offset + 4);
        auto len = loadLength(data.buffer + offset);
        offset += 4 + len;
        data.checkSize(offset);
        if (sets.get(row))
            total_size += len + 1;
    }
    else
    {
        if (null)
        {
            total_size++;
            return;
        }
        data.checkSize(offset + 4);
        auto len = loadLength(data.buffer + offset);
        offset += 4 + len;
        data.checkSize(offset);
        total_size += len + 1;
    }
}

void PlainDecoder::decodeBoolean(
    PaddedPODArray<UInt8> & data, PaddedPODArray<UInt8> & src, const OptionalRowSet & row_set, size_t rows_to_read)
{
    decodeFixedValueInternal(data, src.data(), row_set, rows_to_read);
    offsets.consume(rows_to_read);
}
}
