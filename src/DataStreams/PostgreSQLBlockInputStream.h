#pragma once

#include <Core/Block.h>
#include <DataStreams/IBlockInputStream.h>
#include <Core/ExternalResultDescription.h>

#include <pqxx/pqxx>

namespace DB
{

class PostgreSQLBlockInputStream : public IBlockInputStream
{
public:
    PostgreSQLBlockInputStream(
        std::shared_ptr<pqxx::connection> connection_,
        const std::string & query_str,
        const Block & sample_block,
        const UInt64 max_block_size_);

    String getName() const override { return "PostgreSQL"; }
    Block getHeader() const override { return description.sample_block.cloneEmpty(); }

private:
    using ValueType = ExternalResultDescription::ValueType;

    void readPrefix() override;
    Block readImpl() override;

    void insertValue(IColumn & column, const std::string & value,
        const ExternalResultDescription::ValueType type, const DataTypePtr data_type, size_t idx);
    void insertDefaultValue(IColumn & column, const IColumn & sample_column)
    {
        column.insertFrom(sample_column, 0);
    }
    void getArrayInfo(size_t column_idx, const DataTypePtr data_type);

    const String query_str;
    const UInt64 max_block_size;
    ExternalResultDescription description;

    std::shared_ptr<pqxx::connection> connection;
    std::unique_ptr<pqxx::work> work;
    std::unique_ptr<pqxx::stream_from> stream;

    struct ArrayInfo
    {
        size_t num_dimensions;
        Field default_value;
        std::function<Field(std::string & field)> pqxx_parser;
    };
    std::unordered_map<size_t, ArrayInfo> array_info;
};

}
