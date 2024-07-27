#pragma once
#include "ColumnFilter.h"

#include <iostream>
#include <vector>
#include <Columns/ColumnString.h>
#include <DataTypes/IDataType.h>
#include <Processors/Chunk.h>
#include <Processors/Formats/Impl/Parquet/PageReader.h>
#include <arrow/util/rle_encoding.h>
#include <parquet/column_page.h>
#include <parquet/column_reader.h>
#include <parquet/file_reader.h>
#include <Common/PODArray.h>
namespace parquet
{
class ColumnDescriptor;
}

namespace DB
{

class SelectiveColumnReader;

using SelectiveColumnReaderPtr = std::shared_ptr<SelectiveColumnReader>;

struct ScanSpec
{
    String column_name;
    const parquet::ColumnDescriptor * column_desc = nullptr;
    ColumnFilterPtr filter;
};

class FilterCache
{
public:
    explicit FilterCache(size_t size)
    {
        cache_set.resize(size);
        filter_cache.resize(size);
    }

    bool has(size_t size) { return cache_set.test(size); }

    bool hasNull() const { return exist_null; }

    bool getNull() const { return value_null; }

    void setNull(bool value)
    {
        exist_null = true;
        value_null = value;
    }

    bool get(size_t size) { return filter_cache.test(size); }

    void set(size_t size, bool value)
    {
        cache_set.set(size, true);
        filter_cache.set(size, value);
    }

private:
    boost::dynamic_bitset<> cache_set;
    boost::dynamic_bitset<> filter_cache;
    bool exist_null = false;
    bool value_null = false;
};


struct ScanState
{
    std::shared_ptr<parquet::Page> page;
    PaddedPODArray<Int16> def_levels;
    PaddedPODArray<Int16> rep_levels;
    const uint8_t * buffer = nullptr;
    size_t buffer_size = 0;
    size_t lazy_skip_rows = 0;

    // for dictionary encoding
    PaddedPODArray<UInt32> idx_buffer;
    std::unique_ptr<FilterCache> filter_cache;

    size_t remain_rows = 0;
};

Int32 loadLength(const uint8_t * data);


class PlainDecoder
{
public:
    PlainDecoder(const uint8_t *& buffer_, size_t & remain_rows_) : buffer(buffer_), remain_rows(remain_rows_) { }

    template <typename T>
    void decodeFixedValue(PaddedPODArray<T> & data, RowSet & row_set, size_t rows_to_read)
    {
        const T * start = reinterpret_cast<const T *>(buffer);
        if (row_set.all())
        {
            data.insert_assume_reserved(start, start + rows_to_read);
        }
        else
        {
            size_t rows_read = 0;
            while (rows_read < rows_to_read)
            {
                if (row_set.get(rows_read))
                {
                    data.push_back(start[rows_read]);
                }
                rows_read++;
            }
        }

        buffer += rows_to_read * sizeof(T);
        remain_rows -= rows_to_read;
    }

    void decodeString(ColumnString::Chars & chars, ColumnString::Offsets & offsets, const RowSet & row_set, size_t rows_to_read)
    {
        size_t offset = 0;
        for (size_t i = 0; i < rows_to_read; i++)
        {
            auto len = loadLength(buffer + offset);
            offset += 4;
            if (row_set.get(i))
            {
                chars.insert_assume_reserved(buffer + offset, buffer + offset + len);
                chars.push_back(0);
                offsets.push_back(chars.size());
                offset += len;
            }
            else
            {
                offset += len;
            }
        }
        buffer += offset;
        remain_rows -= rows_to_read;
    }

    template <typename T>
    void decodeFixedValueSpace(PaddedPODArray<T> & data, RowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t rows_to_read)
    {
        size_t rows_read = 0;
        const T * start = reinterpret_cast<const T *>(buffer);
        size_t count = 0;
        if (row_set.all())
        {
            for (size_t i = 0; i < rows_to_read; i++)
            {
                if (null_map[i])
                {
                    data.push_back(0);
                }
                else
                {
                    data.push_back(start[count]);
                    count++;
                }
            }
        }
        else
        {
            while (rows_read < rows_to_read)
            {
                if (row_set.get(rows_read))
                {
                    if (null_map[rows_read])
                    {
                        data.push_back(0);
                    }
                    else
                    {
                        data.push_back(start[count]);
                        count++;
                    }
                }
                rows_read++;
            }
        }
        buffer += count * sizeof(Int64);
        remain_rows -= rows_to_read;
    }

    void decodeStringSpace(
        ColumnString::Chars & chars,
        ColumnString::Offsets & offsets,
        const RowSet & row_set,
        PaddedPODArray<UInt8> & null_map,
        size_t rows_to_read)
    {
        size_t offset = 0;
        for (size_t i = 0; i < rows_to_read; i++)
        {
            if (null_map[i])
            {
                if (row_set.get(i))
                {
                    // null string
                    chars.push_back(0);
                    offsets.push_back(chars.size());
                }
                continue;
            }
            auto len = loadLength(buffer + offset);
            offset += 4;
            if (row_set.get(i))
            {
                chars.insert_assume_reserved(buffer + offset, buffer + offset + len);
                chars.push_back(0);
                offsets.push_back(chars.size());
                offset += len;
            }
            else
            {
                offset += len;
            }
        }
        buffer += offset;
        remain_rows -= rows_to_read;
    }

    size_t calculateStringTotalSize(const uint8_t * data, const RowSet & row_set, const size_t rows_to_read)
    {
        size_t offset = 0;
        size_t total_size = 0;
        for (size_t i = 0; i < rows_to_read; i++)
        {
            addOneString(false, data, offset, row_set, i, total_size);
        }
        return total_size;
    }

    size_t
    calculateStringTotalSizeSpace(const uint8_t * data, const RowSet & row_set, PaddedPODArray<UInt8> & null_map, const size_t rows_to_read)
    {
        size_t offset = 0;
        size_t total_size = 0;
        for (size_t i = 0; i < rows_to_read; i++)
        {
            addOneString(null_map[i], data, offset, row_set, i, total_size);
        }
        return total_size;
    }

private:
    void addOneString(bool null, const uint8_t * data, size_t & offset, const RowSet & row_set, size_t row, size_t & total_size)
    {
        if (null)
        {
            if (row_set.get(row)) total_size++;
            return;
        }
        auto len = loadLength(data + offset);
        chassert(len <= 16);
        offset += 4 + len;
        if (row_set.get(row))
            total_size += len + 1;
    }

    const uint8_t *& buffer;
    size_t & remain_rows;
};

class DictDecoder
{
public:
    DictDecoder(PaddedPODArray<UInt32> & idx_buffer_, size_t & remain_rows_) : idx_buffer(idx_buffer_), remain_rows(remain_rows_) { }

    template <class DictValueType>
    void decodeFixedValue(PaddedPODArray<DictValueType> & dict, PaddedPODArray<DictValueType> & data, RowSet & row_set, size_t rows_to_read)
    {
        size_t rows_read = 0;
        while (rows_read < rows_to_read)
        {
            if (row_set.get(rows_read))
            {
                data.push_back(dict[idx_buffer[rows_read]]);
            }
            rows_read++;
        }
        idx_buffer.resize(0);
        remain_rows -= rows_to_read;
    }

    void decodeString(
        std::vector<String> & dict,
        ColumnString::Chars & chars,
        ColumnString::Offsets & offsets,
        const RowSet & row_set,
        size_t rows_to_read)
    {
        for (size_t i = 0; i < rows_to_read; i++)
        {
            if (row_set.get(i))
            {
                String& value = dict[idx_buffer[i]];
                auto chars_cursor = chars.size();
                chars.resize(chars_cursor + value.size() + 1);
                memcpySmallAllowReadWriteOverflow15(&chars[chars_cursor], value.data(), value.size());
                chars.back() = 0;
                offsets.push_back(chars.size());
            }
        }
        idx_buffer.resize(0);
        remain_rows -= rows_to_read;
    }

    template <class DictValueType>
    void decodeFixedValueSpace(
        PaddedPODArray<DictValueType> & dict,
        PaddedPODArray<DictValueType> & data,
        RowSet & row_set,
        PaddedPODArray<UInt8> & null_map,
        size_t rows_to_read)
    {
        size_t rows_read = 0;
        size_t count = 0;
        while (rows_read < rows_to_read)
        {
            if (row_set.get(rows_read))
            {
                if (null_map[rows_read])
                {
                    data.push_back(0);
                }
                else
                {
                    data.push_back(dict[idx_buffer[count++]]);
                }
            }
            else if (!null_map[rows_read])
                count++;
            rows_read++;
        }
        chassert(count == idx_buffer.size());
        remain_rows -= rows_to_read;
        idx_buffer.resize(0);
    }

    void decodeStringSpace(
        std::vector<String> & dict,
        ColumnString::Chars & chars,
        ColumnString::Offsets & offsets,
        const RowSet & row_set,
        PaddedPODArray<UInt8> & null_map,
        size_t rows_to_read)
    {
        size_t rows_read = 0;
        size_t count = 0;
        while (rows_read < rows_to_read)
        {
            if (row_set.get(rows_read))
            {
                if (null_map[rows_read])
                {
                    chars.push_back(0);
                    offsets.push_back(chars.size());
                }
                else
                {
                    String value = dict[idx_buffer[count]];
                    chars.insert(value.data(), value.data() + value.size());
                    chars.push_back(0);
                    offsets.push_back(chars.size());
                    count++;
                }
            }
            else if (!null_map[rows_read])
                count++;
            rows_read++;
        }
        chassert(count == idx_buffer.size());
        remain_rows -= rows_to_read;
        idx_buffer.resize(0);
    }

private:
    PaddedPODArray<UInt32> & idx_buffer;
    size_t & remain_rows;
};


class SelectiveColumnReader
{
    friend class OptionalColumnReader;

public:
    SelectiveColumnReader(std::unique_ptr<LazyPageReader> page_reader_, const ScanSpec & scan_spec_)
        : page_reader(std::move(page_reader_)), scan_spec(scan_spec_)
    {
    }
    virtual ~SelectiveColumnReader() = default;
    virtual void computeRowSet(RowSet & row_set, size_t rows_to_read) = 0;
    virtual void computeRowSetSpace(RowSet &, PaddedPODArray<UInt8> &, size_t, size_t) { }
    virtual void read(MutableColumnPtr & column, RowSet & row_set, size_t rows_to_read) = 0;
    virtual void readSpace(MutableColumnPtr &, RowSet &, PaddedPODArray<UInt8> &, size_t, size_t) { }
    virtual void readPageIfNeeded();
    void readAndDecodePage()
    {
        readPageIfNeeded();
        decodePage();
    }

    virtual MutableColumnPtr createColumn() = 0;
    const PaddedPODArray<Int16> & getDefinitionLevels()
    {
        readPageIfNeeded();
        return state.def_levels;
    }

    virtual size_t currentRemainRows() const { return state.remain_rows; }

    void skipNulls(size_t rows_to_skip)
    {
        auto skipped = std::min(rows_to_skip, state.remain_rows);
        state.remain_rows -= skipped;
        state.lazy_skip_rows += (rows_to_skip - skipped);
    }

    void skip(size_t rows)
    {
        state.lazy_skip_rows += rows;
        state.lazy_skip_rows = skipValuesInCurrentPage(state.lazy_skip_rows);
        //        std::cerr << "lazy skip " << state.lazy_skip_rows << " rows" << std::endl;
        skipPageIfNeed();
    }

    // skip values in current page, return the number of rows need to lazy skip
    virtual size_t skipValuesInCurrentPage(size_t rows_to_skip) = 0;

    virtual int16_t max_definition_level() const { return scan_spec.column_desc->max_definition_level(); }

    virtual int16_t max_repetition_level() const { return scan_spec.column_desc->max_repetition_level(); }

protected:
    void decodePage();
    virtual void skipPageIfNeed();
    bool readPage();
    void readDataPageV1(const parquet::DataPageV1 & page);
    virtual void readDictPage(const parquet::DictionaryPage &) { }
    virtual void initIndexDecoderIfNeeded() { }
    virtual void createDictDecoder() { }
    virtual void downgradeToPlain() { }

    std::unique_ptr<LazyPageReader> page_reader;
    ScanState state;
    ScanSpec scan_spec;
    std::unique_ptr<PlainDecoder> plain_decoder;
    bool plain = true;
};

template <typename DataType>
class NumberColumnDirectReader : public SelectiveColumnReader
{
public:
    NumberColumnDirectReader(std::unique_ptr<LazyPageReader> page_reader_, ScanSpec scan_spec_);
    ~NumberColumnDirectReader() override = default;
    MutableColumnPtr createColumn() override;
    void computeRowSet(RowSet & row_set, size_t rows_to_read) override;
    void computeRowSetSpace(RowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t null_count, size_t rows_to_read) override;
    void read(MutableColumnPtr & column, RowSet & row_set, size_t rows_to_read) override;
    void readSpace(
        MutableColumnPtr & column, RowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t null_count, size_t rows_to_read) override;
    size_t skipValuesInCurrentPage(size_t rows_to_skip) override;
};

template <typename DataType>
class NumberDictionaryReader : public SelectiveColumnReader
{
public:
    NumberDictionaryReader(std::unique_ptr<LazyPageReader> page_reader_, ScanSpec scan_spec_);
    ~NumberDictionaryReader() override = default;
    void computeRowSet(RowSet & row_set, size_t rows_to_read) override;
    void computeRowSetSpace(RowSet & set, PaddedPODArray<UInt8> & null_map, size_t null_count, size_t rows_to_read) override;
    void read(MutableColumnPtr & column, RowSet & row_set, size_t rows_to_read) override;
    void readSpace(MutableColumnPtr & ptr, RowSet & set, PaddedPODArray<UInt8> & null_map, size_t null_count, size_t size) override;
    MutableColumnPtr createColumn() override { return DataType::ColumnType::create(); }
    size_t skipValuesInCurrentPage(size_t rows_to_skip) override;

protected:
    void readDictPage(const parquet::DictionaryPage & page) override;
    void initIndexDecoderIfNeeded() override
    {
        if (dict.empty())
            return;
        uint8_t bit_width = *state.buffer;
        idx_decoder = arrow::util::RleDecoder(++state.buffer, static_cast<int>(--state.buffer_size), bit_width);
    }
    void nextIdxBatchIfEmpty(size_t rows_to_read);

    void createDictDecoder() override;

    void downgradeToPlain() override;

private:
    arrow::util::RleDecoder idx_decoder;
    std::unique_ptr<DictDecoder> dict_decoder;
    PaddedPODArray<typename DataType::FieldType> dict;
};

void computeRowSetPlainString(const uint8_t * start, RowSet & row_set, ColumnFilterPtr filter, size_t rows_to_read);


void computeRowSetPlainStringSpace(
    const uint8_t * start, RowSet & row_set, ColumnFilterPtr filter, size_t rows_to_read, PaddedPODArray<UInt8> & null_map);


class StringDirectReader : public SelectiveColumnReader
{
public:
    StringDirectReader(std::unique_ptr<LazyPageReader> page_reader_, const ScanSpec & scan_spec_)
        : SelectiveColumnReader(std::move(page_reader_), scan_spec_)
    {
    }

    void computeRowSet(RowSet & row_set, size_t rows_to_read) override
    {
        readAndDecodePage();
        computeRowSetPlainString(state.buffer, row_set, scan_spec.filter, rows_to_read);
    }
    void computeRowSetSpace(RowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t /*null_count*/, size_t rows_to_read) override
    {
        readAndDecodePage();
        computeRowSetPlainStringSpace(state.buffer, row_set, scan_spec.filter, rows_to_read, null_map);
    }
    void read(MutableColumnPtr & column, RowSet & row_set, size_t rows_to_read) override
    {
        ColumnString * string_column = reinterpret_cast<ColumnString *>(column.get());
        size_t total_size = plain_decoder->calculateStringTotalSize(state.buffer, row_set, rows_to_read);
        string_column->getOffsets().reserve(string_column->getOffsets().size() + rows_to_read);
        string_column->getChars().reserve(string_column->getChars().size() + total_size);
        plain_decoder->decodeString(string_column->getChars(), string_column->getOffsets(), row_set, rows_to_read);
    }
    void readSpace(
        MutableColumnPtr & column, RowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t null_count, size_t rows_to_read) override
    {
        ColumnString * string_column = reinterpret_cast<ColumnString *>(column.get());
        size_t total_size = plain_decoder->calculateStringTotalSizeSpace(state.buffer, row_set, null_map, rows_to_read - null_count);
        string_column->getOffsets().reserve(string_column->getOffsets().size() + rows_to_read);
        string_column->getChars().reserve(string_column->getChars().size() + total_size);
        plain_decoder->decodeStringSpace(string_column->getChars(), string_column->getOffsets(), row_set, null_map, rows_to_read);
    }
    size_t skipValuesInCurrentPage(size_t rows_to_skip) override
    {
        if (!state.page || !rows_to_skip)
            return rows_to_skip;
        size_t skipped = std::min(state.remain_rows, rows_to_skip);
        state.remain_rows -= skipped;
        size_t offset = 0;
        for (size_t i = 0; i < skipped; i++)
        {
            auto len = loadLength(state.buffer + offset);
            offset += 4 + len;
        }
        state.buffer += offset;
        return rows_to_skip - skipped;
    }

    MutableColumnPtr createColumn() override { return ColumnString::create(); }
};

class StringDictionaryReader : public SelectiveColumnReader
{
public:
    StringDictionaryReader(std::unique_ptr<LazyPageReader> page_reader_, const ScanSpec & scan_spec_)
        : SelectiveColumnReader(std::move(page_reader_), scan_spec_)
    {
    }

    MutableColumnPtr createColumn() override { return ColumnString::create(); }

    void computeRowSet(RowSet & row_set, size_t rows_to_read) override
    {
        readAndDecodePage();
        chassert(rows_to_read <= state.remain_rows);
        if (plain)
        {
            computeRowSetPlainString(state.buffer, row_set, scan_spec.filter, rows_to_read);
            return;
        }
        nextIdxBatchIfEmpty(rows_to_read);
        if (scan_spec.filter || row_set.any())
        {
            auto & cache = *state.filter_cache;
            for (size_t i = 0; i < rows_to_read; ++i)
            {
                int idx = state.idx_buffer[0];
                if (!cache.has(idx))
                {
                    cache.set(idx, scan_spec.filter->testString(dict[idx]));
                }
                row_set.set(i, cache.get(idx));
            }
        }
    }
    void computeRowSetSpace(RowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t null_count, size_t rows_to_read) override
    {
        readAndDecodePage();
        chassert(rows_to_read <= state.remain_rows);
        if (plain)
        {
            computeRowSetPlainStringSpace(state.buffer, row_set, scan_spec.filter, rows_to_read, null_map);
            return;
        }
        auto nonnull_count = rows_to_read - null_count;
        nextIdxBatchIfEmpty(nonnull_count);
        if (scan_spec.filter || row_set.any())
        {
            auto & cache = *state.filter_cache;
            int count = 0;
            for (size_t i = 0; i < rows_to_read; ++i)
            {
                if (null_map[i])
                {
                    if (!cache.hasNull())
                    {
                        cache.setNull(scan_spec.filter->testNull());
                    }
                    row_set.set(i, cache.getNull());
                }
                else
                {
                    int idx = state.idx_buffer[count++];
                    if (!cache.has(idx))
                    {
                        cache.set(idx, scan_spec.filter->testString(dict[idx]));
                    }
                    row_set.set(i, cache.get(idx));
                }
            }
        }
    }
    void read(MutableColumnPtr & column, RowSet & row_set, size_t rows_to_read) override
    {
        readAndDecodePage();
        ColumnString * string_column = reinterpret_cast<ColumnString *>(column.get());
        nextIdxBatchIfEmpty(rows_to_read);
        if (plain)
        {
            size_t total_size = plain_decoder->calculateStringTotalSize(state.buffer, row_set, rows_to_read);
            string_column->getOffsets().reserve(string_column->getOffsets().size() + rows_to_read);
            string_column->getChars().reserve(string_column->getChars().size() + total_size);
            plain_decoder->decodeString(string_column->getChars(), string_column->getOffsets(), row_set, rows_to_read);
        }
        else
        {
            dict_decoder->decodeString(dict, string_column->getChars(), string_column->getOffsets(), row_set, rows_to_read);
        }
    }
    void readSpace(
        MutableColumnPtr & column, RowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t null_count, size_t rows_to_read) override
    {
        readAndDecodePage();
        ColumnString * string_column = reinterpret_cast<ColumnString *>(column.get());
        auto nonnull_count = rows_to_read - null_count;
        nextIdxBatchIfEmpty(nonnull_count);
        if (plain)
        {
            size_t total_size = plain_decoder->calculateStringTotalSizeSpace(state.buffer, row_set, null_map, rows_to_read);
            string_column->getOffsets().reserve(string_column->getOffsets().size() + rows_to_read);
            string_column->getChars().reserve(string_column->getChars().size() + total_size);
            plain_decoder->decodeStringSpace(string_column->getChars(), string_column->getOffsets(), row_set, null_map, rows_to_read);
        }
        else
        {
            dict_decoder->decodeStringSpace(dict, string_column->getChars(), string_column->getOffsets(), row_set, null_map, rows_to_read);
        }
    }

    size_t skipValuesInCurrentPage(size_t rows_to_skip) override
    {
        if (!state.page || !rows_to_skip)
            return rows_to_skip;
        size_t skipped = std::min(state.remain_rows, rows_to_skip);
        state.remain_rows -= skipped;
        if (plain)
        {
            size_t offset = 0;
            for (size_t i = 0; i < skipped; i++)
            {
                auto len = loadLength(state.buffer + offset);
                offset += 4 + len;
            }
            state.buffer += offset;
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

protected:
    void readDictPage(const parquet::DictionaryPage & page) override
    {
        const auto * dict_data = page.data();
        size_t dict_size = page.num_values();
        dict.reserve(dict_size);
        state.filter_cache = std::make_unique<FilterCache>(dict_size);
        for (size_t i = 0; i < dict_size; i++)
        {
            auto len = loadLength(dict_data);
            dict_data += 4;
            String value;
            value.resize(len);
            memcpy(value.data(), dict_data, len);
            dict.emplace_back(value);
            dict_data += len;
        }
    }
    void initIndexDecoderIfNeeded() override
    {
        if (dict.empty())
            return;
        uint8_t bit_width = *state.buffer;
        idx_decoder = arrow::util::RleDecoder(++state.buffer, static_cast<int>(--state.buffer_size), bit_width);
    }

    /// TODO move to DictDecoder
    void nextIdxBatchIfEmpty(size_t rows_to_read)
    {
        if (!state.idx_buffer.empty() || plain)
            return;
        state.idx_buffer.resize(rows_to_read);
        idx_decoder.GetBatch(state.idx_buffer.data(), static_cast<int>(rows_to_read));
    }

    void createDictDecoder() override { dict_decoder = std::make_unique<DictDecoder>(state.idx_buffer, state.remain_rows); }

    void downgradeToPlain() override
    {
        dict_decoder = nullptr;
        dict.clear();
    }

private:
    std::vector<String> dict;
    std::unique_ptr<DictDecoder> dict_decoder;
    arrow::util::RleDecoder idx_decoder;
};

class ParquetReader;

class RowGroupChunkReader
{
public:
    RowGroupChunkReader(
        ParquetReader * parquetReader,
        std::shared_ptr<parquet::RowGroupMetaData> rowGroupReader,
        std::unordered_map<String, ColumnFilterPtr> filters);
    Chunk readChunk(size_t rows);
    bool hasMoreRows() const { return remain_rows > 0; }

private:
    ParquetReader * parquet_reader;
    std::shared_ptr<parquet::RowGroupMetaData> row_group_meta;
    std::vector<String> filter_columns;
    std::unordered_map<String, SelectiveColumnReaderPtr> reader_columns_mapping;
    std::vector<SelectiveColumnReaderPtr> column_readers;
    std::vector<PaddedPODArray<UInt8>> column_buffers;
    size_t remain_rows = 0;
};

class OptionalColumnReader : public SelectiveColumnReader
{
public:
    OptionalColumnReader(const ScanSpec & scanSpec, const SelectiveColumnReaderPtr child_)
        : SelectiveColumnReader(nullptr, scanSpec), child(child_)
    {
        def_level = child->max_definition_level();
        rep_level = child->max_repetition_level();
    }

    ~OptionalColumnReader() override = default;

    void readPageIfNeeded() override { child->readPageIfNeeded(); }
    MutableColumnPtr createColumn() override;
    size_t currentRemainRows() const override;
    void computeRowSet(RowSet & row_set, size_t rows_to_read) override;
    void read(MutableColumnPtr & column, RowSet & row_set, size_t rows_to_read) override;
    size_t skipValuesInCurrentPage(size_t rows_to_skip) override;
    int16_t max_definition_level() const override { return child->max_definition_level(); }
    int16_t max_repetition_level() const override { return child->max_repetition_level(); }

private:
    void applyLazySkip();

protected:
    void skipPageIfNeed() override;

private:
    void nextBatchNullMapIfNeeded(size_t rows_to_read);
    void cleanNullMap()
    {
        cur_null_count = 0;
        cur_null_map.resize(0);
    }

    SelectiveColumnReaderPtr child;
    PaddedPODArray<UInt8> cur_null_map;
    size_t cur_null_count = 0;
    int def_level = 0;
    int rep_level = 0;
};

class SelectiveColumnReaderFactory
{
public:
    static SelectiveColumnReaderPtr createLeafColumnReader(
        const parquet::ColumnChunkMetaData & column_metadata,
        const parquet::ColumnDescriptor * column_desc,
        std::unique_ptr<LazyPageReader> page_reader,
        ColumnFilterPtr filter);
    static SelectiveColumnReaderPtr createOptionalColumnReader(SelectiveColumnReaderPtr child, ColumnFilterPtr filter);
};
}
