#pragma once
#include "ColumnFilter.h"

#include <vector>
#include <Processors/Chunk.h>
#include <DataTypes/IDataType.h>
#include <parquet/column_page.h>
#include <parquet/column_reader.h>

namespace DB
{
class RowSet
{
};

class SelectiveColumnReader;

using SelectiveColumnReaderPtr = std::shared_ptr<SelectiveColumnReader>;

struct ScanSpec
{
    String column_name;
    DataTypePtr expected_type;
    ColumnFilterPtr filter;
};

class SelectiveColumnReader
{
public:
    virtual ~SelectiveColumnReader();
    virtual void computeRowSet(RowSet row_set, int num_rows) = 0;
    virtual ColumnPtr read(RowSet row_set, int num_rows) = 0;
    virtual void getValues() {};
};

template <class T>
class NumericColumnDirectReader<T> : public SelectiveColumnReader
{
public:
    ~NumericColumnDirectReader() override{};
    void computeRowSet(RowSet row_set, int num_rows) override;
    ColumnPtr read(RowSet row_set, int num_rows) override;

private:
    std::unique_ptr<parquet::PageReader> parquet_page_reader;
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
