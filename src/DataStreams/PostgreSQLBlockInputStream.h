#pragma once

#if !defined(ARCADIA_BUILD)
#include "config_core.h"
#endif

#if USE_LIBPQXX
#include <Core/Block.h>
#include <DataStreams/IBlockInputStream.h>
#include <Core/ExternalResultDescription.h>
#include <Core/Field.h>
#include <pqxx/pqxx>


namespace DB
{
using ConnectionPtr = std::shared_ptr<pqxx::connection>;

class PostgreSQLBlockInputStream : public IBlockInputStream
{
public:
    PostgreSQLBlockInputStream(
        ConnectionPtr connection_,
        const std::string & query_str,
        const Block & sample_block,
        const UInt64 max_block_size_);

    String getName() const override { return "PostgreSQL"; }
    Block getHeader() const override { return description.sample_block.cloneEmpty(); }

private:
    using ValueType = ExternalResultDescription::ValueType;

    void readPrefix() override;
    Block readImpl() override;
    void readSuffix() override;

    void insertValue(IColumn & column, std::string_view value,
        const ExternalResultDescription::ValueType type, const DataTypePtr data_type, size_t idx);
    void insertDefaultValue(IColumn & column, const IColumn & sample_column)
    {
        column.insertFrom(sample_column, 0);
    }
    void prepareArrayInfo(size_t column_idx, const DataTypePtr data_type);

    String query_str;
    const UInt64 max_block_size;
    ExternalResultDescription description;

    ConnectionPtr connection;
    std::unique_ptr<pqxx::read_transaction> tx;
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

#endif
