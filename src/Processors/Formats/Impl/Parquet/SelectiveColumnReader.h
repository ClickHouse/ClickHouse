#pragma once
#include "ColumnFilter.h"

#include <iostream>
#include <vector>
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

private:
    const uint8_t *& buffer;
    size_t & remain_rows;
};

template <typename DictValueType>
class DictDecoder
{
public:
    DictDecoder(PaddedPODArray<DictValueType> & dict_, PaddedPODArray<UInt32> & idx_buffer_, size_t & remain_rows_)
        : dict(dict_), idx_buffer(idx_buffer_), remain_rows(remain_rows_)
    {
    }

    void decodeFixedValue(PaddedPODArray<DictValueType> & data, RowSet & row_set, size_t rows_to_read)
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

    void
    decodeFixedValueSpace(PaddedPODArray<DictValueType> & data, RowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t rows_to_read)
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

private:
    PaddedPODArray<DictValueType> & dict;
    PaddedPODArray<UInt32> & idx_buffer;
    size_t & remain_rows;
};


class SelectiveColumnReader
{
    friend class OptionalColumnReader;

public:
    SelectiveColumnReader(std::unique_ptr<LazyPageReader> page_reader_, const ScanSpec scan_spec_)
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

    void cleanIdxBatch() { return state.idx_buffer.resize(0); }

    void createDictDecoder() override;

    void downgradeToPlain() override;

private:
    arrow::util::RleDecoder idx_decoder;
    std::unique_ptr<DictDecoder<typename DataType::FieldType>> dict_decoder;
    PaddedPODArray<typename DataType::FieldType> dict;
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
