#pragma once
#include "ColumnFilter.h"

#include <vector>
#include <DataTypes/IDataType.h>
#include <Processors/Chunk.h>
#include <parquet/column_page.h>
#include <parquet/column_reader.h>

namespace parquet
{
class ColumnDescriptor;
}

namespace DB
{
class RowSet
{
public:
    inline void set(size_t i, bool value) { }
    size_t offset() { return 0; }
    size_t totalRows() { return 0; }
};

class SelectiveColumnReader;

using SelectiveColumnReaderPtr = std::shared_ptr<SelectiveColumnReader>;

struct ScanSpec
{
    String column_name;
    DataTypePtr expected_type;
    const parquet::ColumnDescriptor * column_desc;
    ColumnFilterPtr filter;
};

class ScanState
{
public:
    std::shared_ptr<parquet::Page> page;
    Int32 offset = 0;
    size_t rows_read = 0;
};

class SelectiveColumnReader
{
public:
    virtual ~SelectiveColumnReader();
    virtual void computeRowSet(RowSet row_set, size_t & rows_to_read) = 0;
    virtual ColumnPtr read(RowSet row_set, size_t & rows_to_read) = 0;
    virtual void getValues() { }

protected:
    void readPage();
    void readPageV1(const parquet::DataPageV1 & page);

    std::unique_ptr<parquet::PageReader> page_reader;
    ScanState state;
    ScanSpec scan_spec;

};



class Int64ColumnDirectReader : public SelectiveColumnReader
{
public:
    ~Int64ColumnDirectReader() override = default;
    void computeRowSet(RowSet row_set, size_t & rows_to_read) override;
    ColumnPtr read(RowSet row_set, size_t & rows_to_read) override;

private:

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
}
