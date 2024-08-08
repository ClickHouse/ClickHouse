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

    inline bool has(size_t size) { return cache_set.test(size); }

    inline bool hasNull() const { return exist_null; }

    inline bool getNull() const { return value_null; }

    inline void setNull(bool value)
    {
        exist_null = true;
        value_null = value;
    }

    inline bool get(size_t size) { return filter_cache.test(size); }

    inline void set(size_t size, bool value)
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
    void decodeFixedValue(PaddedPODArray<T> & data, const OptionalRowSet & row_set, size_t rows_to_read)
    {
        const T * start = reinterpret_cast<const T *>(buffer);
        if (!row_set.has_value())
        {
            data.insert_assume_reserved(start, start + rows_to_read);
        }
        else
        {
            size_t rows_read = 0;
            const auto & sets = row_set.value();
            while (rows_read < rows_to_read)
            {
                if (sets.get(rows_read))
                {
                    data.push_back(start[rows_read]);
                }
                rows_read++;
            }
        }

        buffer += rows_to_read * sizeof(T);
        remain_rows -= rows_to_read;
    }

    void decodeString(ColumnString::Chars & chars, ColumnString::Offsets & offsets, const OptionalRowSet & row_set, size_t rows_to_read)
    {
        size_t offset = 0;
        if (row_set.has_value())
        {
            const auto & sets = row_set.value();
            for (size_t i = 0; i < rows_to_read; i++)
            {
                auto len = loadLength(buffer + offset);
                offset += 4;
                if (sets.get(i))
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
        }
        else
        {
            for (size_t i = 0; i < rows_to_read; i++)
            {
                auto len = loadLength(buffer + offset);
                offset += 4;
                chars.insert_assume_reserved(buffer + offset, buffer + offset + len);
                chars.push_back(0);
                offsets.push_back(chars.size());
                offset += len;
            }
        }
        buffer += offset;
        remain_rows -= rows_to_read;
    }

    template <typename T>
    void decodeFixedValueSpace(PaddedPODArray<T> & data, const OptionalRowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t rows_to_read)
    {
        size_t rows_read = 0;
        const T * start = reinterpret_cast<const T *>(buffer);
        size_t count = 0;
        if (!row_set.has_value())
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
            const auto & sets = row_set.value();
            while (rows_read < rows_to_read)
            {
                if (sets.get(rows_read))
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
        const OptionalRowSet & row_set,
        PaddedPODArray<UInt8> & null_map,
        size_t rows_to_read)
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
                        offsets.push_back(chars.size());
                    }
                    continue;
                }
                auto len = loadLength(buffer + offset);
                offset += 4;
                if (sets.get(i))
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
        }
        else
        {
            for (size_t i = 0; i < rows_to_read; i++)
            {
                if (null_map[i])
                {
                    chars.push_back(0);
                    offsets.push_back(chars.size());
                    continue;
                }
                auto len = loadLength(buffer + offset);
                offset += 4;
                chars.insert_assume_reserved(buffer + offset, buffer + offset + len);
                chars.push_back(0);
                offsets.push_back(chars.size());
                offset += len;
            }
        }
        buffer += offset;
        remain_rows -= rows_to_read;
    }

    size_t calculateStringTotalSize(const uint8_t * data, const OptionalRowSet & row_set, const size_t rows_to_read)
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
    calculateStringTotalSizeSpace(const uint8_t * data, const OptionalRowSet & row_set, PaddedPODArray<UInt8> & null_map, const size_t rows_to_read)
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
    void addOneString(bool null, const uint8_t * data, size_t & offset, const OptionalRowSet & row_set, size_t row, size_t & total_size)
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
            auto len = loadLength(data + offset);
            offset += 4 + len;
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
            auto len = loadLength(data + offset);
            offset += 4 + len;
            total_size += len + 1;
        }
    }

    const uint8_t *& buffer;
    size_t & remain_rows;
};

class DictDecoder
{
public:
    DictDecoder(PaddedPODArray<UInt32> & idx_buffer_, size_t & remain_rows_) : idx_buffer(idx_buffer_), remain_rows(remain_rows_) { }

    template <class DictValueType>
    void decodeFixedValue(PaddedPODArray<DictValueType> & dict, PaddedPODArray<DictValueType> & data, const OptionalRowSet & row_set, size_t rows_to_read);

    void decodeString(
        std::vector<String> & dict,
        ColumnString::Chars & chars,
        ColumnString::Offsets & offsets,
        const OptionalRowSet & row_set,
        size_t rows_to_read);

    template <class DictValueType>
    void decodeFixedValueSpace(
        PaddedPODArray<DictValueType> & dict,
        PaddedPODArray<DictValueType> & data,
        const OptionalRowSet & row_set,
        PaddedPODArray<UInt8> & null_map,
        size_t rows_to_read);

    void decodeStringSpace(
        std::vector<String> & dict,
        ColumnString::Chars & chars,
        ColumnString::Offsets & offsets,
        const OptionalRowSet & row_set,
        PaddedPODArray<UInt8> & null_map,
        size_t rows_to_read);

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
    virtual void computeRowSet(std::optional<RowSet> & row_set, size_t rows_to_read) = 0;
    virtual void computeRowSetSpace(OptionalRowSet &, PaddedPODArray<UInt8> &, size_t, size_t) { }
    virtual void read(MutableColumnPtr & column, const OptionalRowSet & row_set, size_t rows_to_read) = 0;
    virtual void readSpace(MutableColumnPtr &, const OptionalRowSet &, PaddedPODArray<UInt8> &, size_t, size_t) { }
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

    void skipNulls(size_t rows_to_skip);

    void skip(size_t rows);

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
    void computeRowSet(OptionalRowSet & row_set, size_t rows_to_read) override;
    void computeRowSetSpace(OptionalRowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t null_count, size_t rows_to_read) override;
    void read(MutableColumnPtr & column, const OptionalRowSet & row_set, size_t rows_to_read) override;
    void readSpace(
        MutableColumnPtr & column, const OptionalRowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t null_count, size_t rows_to_read) override;
    size_t skipValuesInCurrentPage(size_t rows_to_skip) override;
};

template <typename DataType>
class NumberDictionaryReader : public SelectiveColumnReader
{
public:
    NumberDictionaryReader(std::unique_ptr<LazyPageReader> page_reader_, ScanSpec scan_spec_);
    ~NumberDictionaryReader() override = default;
    void computeRowSet(OptionalRowSet & row_set, size_t rows_to_read) override;
    void computeRowSetSpace(OptionalRowSet & set, PaddedPODArray<UInt8> & null_map, size_t null_count, size_t rows_to_read) override;
    void read(MutableColumnPtr & column, const OptionalRowSet & row_set, size_t rows_to_read) override;
    void readSpace(MutableColumnPtr & ptr, const OptionalRowSet & set, PaddedPODArray<UInt8> & null_map, size_t null_count, size_t size) override;
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

void computeRowSetPlainString(const uint8_t * start, OptionalRowSet & row_set, ColumnFilterPtr filter, size_t rows_to_read);


void computeRowSetPlainStringSpace(
    const uint8_t * start, OptionalRowSet & row_set, ColumnFilterPtr filter, size_t rows_to_read, PaddedPODArray<UInt8> & null_map);


class StringDirectReader : public SelectiveColumnReader
{
public:
    StringDirectReader(std::unique_ptr<LazyPageReader> page_reader_, const ScanSpec & scan_spec_)
        : SelectiveColumnReader(std::move(page_reader_), scan_spec_)
    {
    }

    void computeRowSet(OptionalRowSet & row_set, size_t rows_to_read) override;
    void computeRowSetSpace(OptionalRowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t /*null_count*/, size_t rows_to_read) override;
    void read(MutableColumnPtr & column, const OptionalRowSet & row_set, size_t rows_to_read) override;

    void readSpace(
        MutableColumnPtr & column, const OptionalRowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t null_count, size_t rows_to_read) override;

    size_t skipValuesInCurrentPage(size_t rows_to_skip) override;

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

    void computeRowSet(OptionalRowSet & row_set, size_t rows_to_read) override;

    void computeRowSetSpace(OptionalRowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t null_count, size_t rows_to_read) override;

    void read(MutableColumnPtr & column, const OptionalRowSet & row_set, size_t rows_to_read) override;

    void readSpace(MutableColumnPtr & column, const OptionalRowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t null_count, size_t rows_to_read) override;

    size_t skipValuesInCurrentPage(size_t rows_to_skip) override;

protected:
    void readDictPage(const parquet::DictionaryPage & page) override;

    void initIndexDecoderIfNeeded() override;

    /// TODO move to DictDecoder
    void nextIdxBatchIfEmpty(size_t rows_to_read);

    void createDictDecoder() override { dict_decoder = std::make_unique<DictDecoder>(state.idx_buffer, state.remain_rows); }

    void downgradeToPlain() override;

private:
    std::vector<String> dict;
    std::unique_ptr<DictDecoder> dict_decoder;
    arrow::util::RleDecoder idx_decoder;
};

class ParquetReader;


class RowGroupChunkReader
{
public:
    struct ReadMetrics
    {
        size_t output_rows = 0;
        size_t filtered_rows = 0;
        size_t skipped_rows = 0;
    };
    RowGroupChunkReader(
        ParquetReader * parquetReader,
        std::shared_ptr<parquet::RowGroupMetaData> rowGroupReader,
        std::unordered_map<String, ColumnFilterPtr> filters);
    Chunk readChunk(size_t rows);
    bool hasMoreRows() const { return remain_rows > 0; }
    void printMetrics(std::ostream & out) const
    {
        out << "metrics.output_rows: " << metrics.output_rows << "\n";
        out << "metrics.filtered_rows: " << metrics.filtered_rows << "\n";
        out << "metrics.skipped_rows: " << metrics.skipped_rows << "\n";
    }
private:
    ParquetReader * parquet_reader;
    std::shared_ptr<parquet::RowGroupMetaData> row_group_meta;
    std::vector<String> filter_columns;
    std::unordered_map<String, SelectiveColumnReaderPtr> reader_columns_mapping;
    std::vector<SelectiveColumnReaderPtr> column_readers;
    std::vector<PaddedPODArray<UInt8>> column_buffers;
    size_t remain_rows = 0;
    ReadMetrics metrics;
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
    void computeRowSet(OptionalRowSet & row_set, size_t rows_to_read) override;
    void read(MutableColumnPtr & column, const OptionalRowSet & row_set, size_t rows_to_read) override;
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
