#pragma once
#include <ostream>
#include <Processors/Formats/Impl/Parquet/ColumnFilter.h>
#include <DataTypes/IDataType.h>

namespace parquet
{
    class ColumnDescriptor;
    namespace schema
    {
        class Node;
        using NodePtr = std::shared_ptr<Node>;
    }
}

namespace DB
{
class SelectiveColumnReader;
using SelectiveColumnReaderPtr = std::shared_ptr<SelectiveColumnReader>;
class LazyPageReader;
struct RowGroupContext;

using PageReaderCreator = std::function<std::unique_ptr<LazyPageReader>()>;

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
        Builder& pageReader(PageReaderCreator page_reader_creator);
        SelectiveColumnReaderPtr build();
    private:
        bool dictionary_ = false;
        bool nullable_ = false;
        const parquet::ColumnDescriptor * column_descriptor_ = nullptr;
        DataTypePtr target_type_ = nullptr;
        PageReaderCreator page_reader_creator = nullptr;
        std::unique_ptr<LazyPageReader> page_reader_ = nullptr;
        ColumnFilterPtr filter_ = nullptr;
    };

    static Builder builder();
};

class ColumnReaderBuilder
{
public:
    ColumnReaderBuilder(
        const Block & requiredColumns,
        const RowGroupContext & context,
        const std::unordered_map<String, ColumnFilterPtr> & inplaceFilterMapping,
        const std::unordered_set<String> & predicateColumns);
    SelectiveColumnReaderPtr buildReader(parquet::schema::NodePtr node, const DataTypePtr & target_type, int def_level = 0, int rep_level = 0);
private:
    const Block& required_columns;
    const RowGroupContext& context;
    const std::unordered_map<String, ColumnFilterPtr>& inplace_filter_mapping;
    const std::unordered_set<String>& predicate_columns;
};

}
