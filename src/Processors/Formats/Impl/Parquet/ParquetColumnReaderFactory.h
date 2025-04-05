#pragma once
#include <ostream>
#include <Processors/Formats/Impl/Parquet/ColumnFilter.h>
#include <DataTypes/IDataType.h>
#include <parquet/metadata.h>

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
        Builder& isOptional(bool is_optional);
        Builder& columnDescriptor(const parquet::ColumnDescriptor * column_descr);
        Builder& filter(const ColumnFilterPtr & filter);
        Builder& targetType(const DataTypePtr & target_type);
        Builder& pageReader(PageReaderCreator page_reader_creator);
        Builder& columnChunkMeta(std::unique_ptr<parquet::ColumnChunkMetaData> column_chunk_meta);
        SelectiveColumnReaderPtr build();
    private:
        bool dictionary_ = false;
        bool nullable_ = false;
        // is optional data in parquet file
        bool is_optional_ = false;
        const parquet::ColumnDescriptor * column_descriptor_ = nullptr;
        DataTypePtr target_type_ = nullptr;
        PageReaderCreator page_reader_creator = nullptr;
        std::unique_ptr<LazyPageReader> page_reader_;
        ColumnFilterPtr filter_ = nullptr;
        std::unique_ptr<parquet::ColumnChunkMetaData> column_chunk_meta_ = nullptr;
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
    SelectiveColumnReaderPtr buildReader(parquet::schema::NodePtr node, const DataTypePtr & target_type, int def_level = 0, int rep_level = 0, bool is_key = false);
private:
    const Block& required_columns;
    const RowGroupContext& context;
    const std::unordered_map<String, ColumnFilterPtr>& inplace_filter_mapping;
    const std::unordered_set<String>& predicate_columns;
};

}
