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
    void set(size_t i, bool value) { }
    bool get(size_t i) { return true; }
    size_t offset() { return 0; }
    size_t addOffset(size_t i);
    size_t totalRows() { return 0; }
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
    const uint8_t * buffer = nullptr;
    size_t rows_read = 0;
    size_t remain_rows = 0;
};

class SelectiveColumnReader
{
public:
    virtual ~SelectiveColumnReader();
    virtual void computeRowSet(RowSet row_set, size_t & rows_to_read) = 0;
    virtual void read(MutableColumnPtr & column, RowSet row_set, size_t & rows_to_read) = 0;
    virtual void getValues() { }
    const PaddedPODArray<Int16> & getDefenitionLevels()
    {
        readPageIfNeeded();
        return state.def_levels;
    }

protected:
    void readPageIfNeeded();
    void readPage();
    void readDataPageV1(const parquet::DataPageV1 & page);
    void readDictPage(const parquet::DictionaryPage & page);

    std::unique_ptr<parquet::PageReader> page_reader;
    ScanState state;
    ScanSpec scan_spec;
};


class Int64ColumnDirectReader : public SelectiveColumnReader
{
public:
    ~Int64ColumnDirectReader() override = default;
    void computeRowSet(RowSet row_set, size_t & rows_to_read) override;
    void read(MutableColumnPtr & column, RowSet row_set, size_t & rows_to_read) override;
};

class RowGroupChunkReader
{
public:
    Chunk readChunk(int rows);

private:
    std::vector<String> filter_columns;
    std::unordered_map<String, SelectiveColumnReaderPtr> reader_columns_mapping;
    std::vector<SelectiveColumnReaderPtr> column_readers;
};

class OptionalColumnReader : public SelectiveColumnReader
{
public:
    void computeRowSet(RowSet row_set, size_t & rows_to_read) override;
    void read(MutableColumnPtr & column, RowSet row_set, size_t & rows_to_read) override;

private:
    SelectiveColumnReaderPtr child;
};
}
