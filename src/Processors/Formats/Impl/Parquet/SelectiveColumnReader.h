#pragma once
#include "ColumnFilter.h"

#include <vector>
#include <DataTypes/IDataType.h>
#include <Processors/Chunk.h>
#include <parquet/column_page.h>
#include <parquet/column_reader.h>
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
        false;
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
    DataTypePtr expected_type;
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
    virtual ~SelectiveColumnReader() = 0;
    virtual void computeRowSet(RowSet& row_set, size_t rows_to_read) = 0;
    virtual void computeRowSetSpace(RowSet& row_set, PaddedPODArray<UInt8>& null_map, size_t rows_to_read) {};
    virtual void read(MutableColumnPtr & column, RowSet& row_set, size_t rows_to_read) = 0;
    virtual void readSpace(MutableColumnPtr & column, RowSet& row_set, PaddedPODArray<UInt8>& null_map, size_t rows_to_read) {};
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

protected:
    void readPage();
    void readDataPageV1(const parquet::DataPageV1 & page);
    void readDictPage(const parquet::DictionaryPage & page) {}
    int16_t max_definition_level() const
    {
        return scan_spec.column_desc->max_definition_level();
    }

    int16_t max_repetition_level() const
    {
        return scan_spec.column_desc->max_repetition_level();
    }

    std::unique_ptr<parquet::PageReader> page_reader;
    ScanState state;
    ScanSpec scan_spec;
};


class Int64ColumnDirectReader : public SelectiveColumnReader
{
public:
    ~Int64ColumnDirectReader() override = default;
    MutableColumnPtr createColumn() override;
    void computeRowSet(RowSet& row_set, size_t offset, size_t value_offset, size_t rows_to_read) override;
    void computeRowSetSpace(RowSet & row_set, PaddedPODArray<UInt8> & null_map, size_t rows_to_read) override;
    void read(MutableColumnPtr & column, RowSet& row_set, size_t rows_to_read) override;
    void readSpace(MutableColumnPtr & column, RowSet & row_set, PaddedPODArray<UInt8>& null_map, size_t rows_to_read) override;
    void skip(size_t rows) override;
};

class RowGroupChunkReader
{
public:
    Chunk readChunk(size_t rows);

private:
    std::vector<String> filter_columns;
    std::unordered_map<String, SelectiveColumnReaderPtr> reader_columns_mapping;
    std::vector<SelectiveColumnReaderPtr> column_readers;
};

class OptionalColumnReader : public SelectiveColumnReader
{
public:
    ~OptionalColumnReader() override {}
    MutableColumnPtr createColumn() override;
    size_t currentRemainRows() const override;
    void computeRowSet(RowSet& row_set, size_t rows_to_read) override;
    void read(MutableColumnPtr & column, RowSet& row_set, size_t offset, size_t rows_to_read) override;
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
}
