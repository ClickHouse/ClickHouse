#pragma once
#include "ColumnFilter.h"

#include <vector>
#include <DataTypes/IDataType.h>
#include <Processors/Chunk.h>
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
class RowSet
{
public:
    explicit RowSet(size_t maxRows) : max_rows_(maxRows)
    {
        bitset.flip();
    }

    void set(size_t i, bool value) { bitset.set(i, value);}
    bool get(size_t i) { return bitset.test(i); }
    size_t totalRows() const { return max_rows_; }
    bool none() const
    {
        size_t count =0;
        for (size_t i = 0; i < max_rows_; i++)
        {
            if (bitset.test(i))
            {
                count++;
            }
        }
        if (!count)
            return true;
        return false;
    }

private:
    size_t max_rows_ = 0;
    std::bitset<8192> bitset;
};

class SelectiveColumnReader;

using SelectiveColumnReaderPtr = std::shared_ptr<SelectiveColumnReader>;

struct ScanSpec
{
    String column_name;
    const parquet::ColumnDescriptor * column_desc = nullptr;
    ColumnFilterPtr filter;
};

class ScanState
{
public:
    std::shared_ptr<parquet::Page> page;
    PaddedPODArray<Int16> def_levels;
    PaddedPODArray<Int16> rep_levels;
    const uint8_t * buffer;
    size_t remain_rows = 0;
};

class SelectiveColumnReader
{
public:
    SelectiveColumnReader(std::unique_ptr<parquet::PageReader> page_reader_, const ScanSpec scan_spec_)
        : page_reader(std::move(page_reader_)),  scan_spec(scan_spec_)
    {
    }
    virtual ~SelectiveColumnReader() = default;
    virtual void computeRowSet(RowSet& row_set, size_t rows_to_read) = 0;
    virtual void computeRowSetSpace(RowSet& , PaddedPODArray<UInt8>& , size_t ) {}
    virtual void read(MutableColumnPtr & column, RowSet& row_set, size_t rows_to_read) = 0;
    virtual void readSpace(MutableColumnPtr & , RowSet& , PaddedPODArray<UInt8>& , size_t ) {}
    virtual void getValues() { }
    void readPageIfNeeded();

    virtual MutableColumnPtr createColumn() = 0;
    const PaddedPODArray<Int16> & getDefinitionLevels()
    {
        readPageIfNeeded();
        return state.def_levels;
    }

    virtual size_t currentRemainRows() const
    {
        return state.remain_rows;
    }

    void skipNulls(size_t rows_to_skip)
    {
        state.remain_rows -= rows_to_skip;
    }

    virtual void skip(size_t rows) = 0;

    int16_t max_definition_level() const
    {
        return scan_spec.column_desc->max_definition_level();
    }

    int16_t max_repetition_level() const
    {
        return scan_spec.column_desc->max_repetition_level();
    }

protected:
    void readPage();
    void readDataPageV1(const parquet::DataPageV1 & page);
    void readDictPage(const parquet::DictionaryPage & ) {}


    std::unique_ptr<parquet::PageReader> page_reader;
    ScanState state;
    ScanSpec scan_spec;
};

template <typename DataType>
class Int64ColumnDirectReader : public SelectiveColumnReader
{
public:
    Int64ColumnDirectReader(std::unique_ptr<parquet::PageReader> page_reader_, ScanSpec scan_spec_);
    ~Int64ColumnDirectReader() override { }
    MutableColumnPtr createColumn() override;
    void computeRowSet(RowSet& row_set, size_t rows_to_read) override;
    void computeRowSetSpace(RowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t rows_to_read) override;
    void read(MutableColumnPtr & column, RowSet& row_set, size_t rows_to_read) override;
    void readSpace(MutableColumnPtr & column, RowSet & row_set, PaddedPODArray<UInt8>& null_map, size_t rows_to_read) override;
    void skip(size_t rows) override;
};

class ParquetReader;

class RowGroupChunkReader
{
public:
    RowGroupChunkReader(ParquetReader * parquetReader,
                        std::shared_ptr<parquet::RowGroupReader> rowGroupReader,
                        std::unordered_map<String, ColumnFilterPtr> filters);
    Chunk readChunk(size_t rows);
    bool hasMoreRows() const
    {
        return remain_rows > 0;
    }

private:
    ParquetReader * parquet_reader;
    std::shared_ptr<parquet::RowGroupReader> row_group_reader;
    std::vector<String> filter_columns;
    std::unordered_map<String, SelectiveColumnReaderPtr> reader_columns_mapping;
    std::vector<SelectiveColumnReaderPtr> column_readers;
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
    MutableColumnPtr createColumn() override;
    size_t currentRemainRows() const override;
    void computeRowSet(RowSet& row_set, size_t rows_to_read) override;
    void read(MutableColumnPtr & column, RowSet& row_set, size_t rows_to_read) override;
    void skip(size_t rows) override;

private:
    void nextBatchNullMap(size_t rows_to_read);
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
    static SelectiveColumnReaderPtr createLeafColumnReader(const parquet::ColumnChunkMetaData& column_metadata, const parquet::ColumnDescriptor * column_desc, std::unique_ptr<parquet::PageReader> page_reader, ColumnFilterPtr filter);
    static SelectiveColumnReaderPtr createOptionalColumnReader(SelectiveColumnReaderPtr child, ColumnFilterPtr filter);
};
}
