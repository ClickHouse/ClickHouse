#pragma once
#include <ostream>
#include <Processors/Formats/Impl/Parquet/ColumnFilter.h>
#include <DataTypes/IDataType.h>

namespace parquet
{
    class ColumnDescriptor;
}

namespace DB
{
class SelectiveColumnReader;
using SelectiveColumnReaderPtr = std::shared_ptr<SelectiveColumnReader>;
class LazyPageReader;

class ParquetColumnReaderFactory
{
public:
    class Builder
    {
    public:
        Builder& dictionary(bool dictionary);
        Builder& nullable(bool nullable);
        Builder& columnDescriptor(const parquet::ColumnDescriptor * columnDescr);
        Builder& filter(const ColumnFilterPtr & filter);
        Builder& targetType(const DataTypePtr & target_type);
        Builder& pageReader(std::unique_ptr<LazyPageReader> page_reader);
        SelectiveColumnReaderPtr build();
    private:
        bool dictionary_ = false;
        bool nullable_ = false;
        const parquet::ColumnDescriptor * column_descriptor_ = nullptr;
        DataTypePtr target_type_ = nullptr;
        std::unique_ptr<LazyPageReader> page_reader_ = nullptr;
        ColumnFilterPtr filter_ = nullptr;
    };

    static Builder builder();
};



}
