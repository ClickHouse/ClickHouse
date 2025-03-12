#include "SelectiveColumnReader.h"

#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionHelpers.h>
#include <Common/assert_cast.h>

namespace DB
{
void StringDictionaryReader::nextIdxBatchIfEmpty(size_t rows_to_read)
{
    if (!state.idx_buffer.empty() || plain)
        return;
    if (!rows_to_read)
        return;
    state.idx_buffer.resize(rows_to_read);
    size_t count [[maybe_unused]] = idx_decoder.GetBatch(state.idx_buffer.data(), static_cast<int>(rows_to_read));
    chassert(count == rows_to_read);
}

void StringDictionaryReader::initIndexDecoderIfNeeded()
{
    if (dict.empty())
        return;
    state.data.checkSize(1);
    uint8_t bit_width = *state.data.buffer;
    state.data.consume(1);
    idx_decoder = arrow::util::RleDecoder(state.data.buffer, static_cast<int>(state.data.buffer_size), bit_width);
}

void StringDictionaryReader::readDictPage(const parquet::DictionaryPage & page)
{
    const auto * dict_data = page.data();
    size_t dict_size = page.num_values();
    dict.reserve(dict_size);
    state.filter_cache = std::make_unique<FilterCache>(dict_size);
    for (size_t i = 0; i < dict_size; i++)
    {
        auto len = loadLength(dict_data);
        dict_data += 4;
        if (len)
        {
            String value;
            value.resize(len);
            memcpy(value.data(), dict_data, len);
            dict.emplace_back(value);
        }
        else
            dict.emplace_back("");
        dict_data += len;
    }
    dict_page_read = true;
}

size_t StringDictionaryReader::skipValuesInCurrentPage(size_t rows_to_skip)
{
    if (!state.page || !rows_to_skip)
        return rows_to_skip;
    size_t skipped = std::min(state.offsets.remain_rows, rows_to_skip);
    state.offsets.consume(skipped);
    if (plain)
    {
        size_t offset = 0;
        for (size_t i = 0; i < skipped; i++)
        {
            auto len = loadLength(state.data.buffer + offset);
            offset += 4 + len;
        }
        state.data.checkAndConsume(offset);
    }
    else
    {
        if (!state.idx_buffer.empty())
        {
            // only support skip all
            chassert(state.idx_buffer.size() == skipped);
            state.idx_buffer.resize(0);
        }
        else
        {
            state.idx_buffer.resize(skipped);
            idx_decoder.GetBatch(state.idx_buffer.data(), static_cast<int>(skipped));
            state.idx_buffer.resize(0);
        }
    }
    return rows_to_skip - skipped;
}

void StringDictionaryReader::readSpace(
    MutableColumnPtr & column, OptionalRowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t null_count, size_t rows_to_read)
{
    size_t rows_read = 0;
    while (rows_read < rows_to_read)
    {
        auto rows_can_read = std::min(rows_to_read - rows_read, state.offsets.remain_rows);
        readAndDecodePageIfNeeded();
        ColumnString * string_column = reinterpret_cast<ColumnString *>(column.get());
        if (plain)
        {
            size_t total_size = plain_decoder->calculateStringTotalSizeSpace(state.data, row_set, null_map, rows_can_read);
            string_column->getOffsets().reserve(string_column->getOffsets().size() + rows_can_read);
            string_column->getChars().reserve(string_column->getChars().size() + total_size);
            plain_decoder->decodeStringSpace(string_column->getChars(), string_column->getOffsets(), row_set, null_map, rows_can_read);
        }
        else
        {
            auto nonnull_count = rows_can_read - null_count;
            nextIdxBatchIfEmpty(nonnull_count);
            dict_decoder->decodeStringSpace(dict, string_column->getChars(), string_column->getOffsets(), row_set, null_map, rows_to_read);
        }
        if (row_set)
            row_set->addOffset(rows_can_read);
        rows_read += rows_can_read;
    }
}

void StringDictionaryReader::read(MutableColumnPtr & column, OptionalRowSet & row_set, size_t rows_to_read)
{
    size_t rows_read = 0;
    while (rows_read < rows_to_read)
    {
        auto rows_can_read = std::min(rows_to_read - rows_read, state.offsets.remain_rows);
        readAndDecodePageIfNeeded();
        ColumnString * string_column = reinterpret_cast<ColumnString *>(column.get());
        if (plain)
        {
            size_t total_size = plain_decoder->calculateStringTotalSize(state.data, row_set, rows_can_read);
            string_column->getOffsets().reserve(string_column->getOffsets().size() + rows_can_read);
            string_column->getChars().reserve(string_column->getChars().size() + total_size);
            plain_decoder->decodeString(string_column->getChars(), string_column->getOffsets(), row_set, rows_can_read);
        }
        else
        {
            nextIdxBatchIfEmpty(rows_can_read);
            dict_decoder->decodeString(dict, string_column->getChars(), string_column->getOffsets(), row_set, rows_can_read);
        }
        if (row_set)
            row_set->addOffset(rows_can_read);
        rows_read += rows_can_read;
    }
}

void StringDictionaryReader::computeRowSetSpace(
    OptionalRowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t null_count, size_t rows_to_read)
{
    readAndDecodePageIfNeeded();
    chassert(rows_to_read <= state.offsets.remain_rows);
    if (plain)
    {
        computeRowSetPlainStringSpace(state.data.buffer, row_set, scan_spec.filter, rows_to_read, null_map);
        return;
    }
    auto nonnull_count = rows_to_read - null_count;
    nextIdxBatchIfEmpty(nonnull_count);
    if (scan_spec.filter || row_set.has_value())
    {
        auto & cache = *state.filter_cache;
        auto & sets = row_set.value();
        int count = 0;
        for (size_t i = 0; i < rows_to_read; ++i)
        {
            if (null_map[i])
            {
                if (!cache.hasNull())
                {
                    cache.setNull(scan_spec.filter->testNull());
                }
                sets.set(i, cache.getNull());
            }
            else
            {
                int idx = state.idx_buffer[count++];
                if (!cache.has(idx))
                {
                    cache.set(idx, scan_spec.filter->testString(dict[idx]));
                }
                sets.set(i, cache.get(idx));
            }
        }
    }
}

void StringDictionaryReader::computeRowSet(OptionalRowSet & row_set, size_t rows_to_read)
{
    if (!scan_spec.filter || !row_set.has_value())
        return;
    readAndDecodePageIfNeeded();
    chassert(rows_to_read <= state.offsets.remain_rows);
    if (plain)
    {
        computeRowSetPlainString(state.data.buffer, row_set, scan_spec.filter, rows_to_read);
        return;
    }
    nextIdxBatchIfEmpty(rows_to_read);
    auto & cache = *state.filter_cache;
    for (size_t i = 0; i < rows_to_read; ++i)
    {
        auto & sets = row_set.value();
        int idx = state.idx_buffer[i];
        if (!cache.has(idx))
        {
            cache.set(idx, scan_spec.filter->testString(dict[idx]));
        }
        sets.set(i, cache.get(idx));
    }
}

void StringDictionaryReader::downgradeToPlain()
{
    dict_decoder = nullptr;
    dict.clear();
}
DataTypePtr StringDictionaryReader::getResultType()
{
    return std::make_shared<DataTypeString>();
}

size_t StringDirectReader::skipValuesInCurrentPage(size_t rows_to_skip)
{
    if (!state.page || !rows_to_skip)
        return rows_to_skip;
    size_t skipped = std::min(state.offsets.remain_rows, rows_to_skip);
    state.offsets.consume(skipped);
    size_t offset = 0;
    for (size_t i = 0; i < skipped; i++)
    {
        auto len = loadLength(state.data.buffer + offset);
        offset += 4 + len;
    }
    state.data.checkAndConsume(offset);
    return rows_to_skip - skipped;
}

void StringDirectReader::readSpace(
    MutableColumnPtr & column, OptionalRowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t null_count, size_t rows_to_read)
{
    size_t rows_read = 0;
    while (rows_read < rows_to_read)
    {
        auto rows_can_read = std::min(rows_to_read - rows_read, state.offsets.remain_rows);

        readAndDecodePageIfNeeded();
        ColumnString * string_column = reinterpret_cast<ColumnString *>(column.get());
        size_t total_size = plain_decoder->calculateStringTotalSizeSpace(state.data, row_set, null_map, rows_to_read - null_count);
        string_column->getOffsets().reserve(string_column->getOffsets().size() + rows_to_read);
        string_column->getChars().reserve(string_column->getChars().size() + total_size);
        plain_decoder->decodeStringSpace(string_column->getChars(), string_column->getOffsets(), row_set, null_map, rows_to_read);
        if (row_set)
            row_set->addOffset(rows_can_read);
        rows_read += rows_can_read;
    }
}

void StringDirectReader::read(MutableColumnPtr & column, OptionalRowSet & row_set, size_t rows_to_read)
{
    size_t rows_read = 0;
    while (rows_read < rows_to_read)
    {
        auto rows_can_read = std::min(rows_to_read - rows_read, state.offsets.remain_rows);
        readAndDecodePageIfNeeded();
        ColumnString * string_column = reinterpret_cast<ColumnString *>(column.get());
        size_t total_size = plain_decoder->calculateStringTotalSize(state.data, row_set, rows_can_read);
        string_column->getOffsets().reserve(string_column->getOffsets().size() + rows_can_read);
        string_column->getChars().reserve(string_column->getChars().size() + total_size);
        plain_decoder->decodeString(string_column->getChars(), string_column->getOffsets(), row_set, rows_can_read);
        if (row_set)
            row_set->addOffset(rows_can_read);
        rows_read += rows_can_read;
    }
}

void StringDirectReader::computeRowSetSpace(OptionalRowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t, size_t rows_to_read)
{
    readAndDecodePageIfNeeded();
    computeRowSetPlainStringSpace(state.data.buffer, row_set, scan_spec.filter, rows_to_read, null_map);
}

void StringDirectReader::computeRowSet(OptionalRowSet & row_set, size_t rows_to_read)
{
    readAndDecodePageIfNeeded();
    computeRowSetPlainString(state.data.buffer, row_set, scan_spec.filter, rows_to_read);
}
DataTypePtr StringDirectReader::getResultType()
{
    return std::make_shared<DataTypeString>();
}
}
