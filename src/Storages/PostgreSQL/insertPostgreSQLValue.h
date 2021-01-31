#pragma once

#include <Core/Block.h>
#include <DataStreams/IBlockInputStream.h>
#include <Core/ExternalResultDescription.h>
#include <Core/Field.h>
#include <pqxx/pqxx>


namespace DB
{

struct PostgreSQLArrayInfo
{
    size_t num_dimensions;
    Field default_value;
    std::function<Field(std::string & field)> pqxx_parser;
};


void insertPostgreSQLValue(
        IColumn & column, std::string_view value,
        const ExternalResultDescription::ValueType type, const DataTypePtr data_type,
        std::unordered_map<size_t, PostgreSQLArrayInfo> & array_info, size_t idx);

void preparePostgreSQLArrayInfo(
        std::unordered_map<size_t, PostgreSQLArrayInfo> & array_info, size_t column_idx, const DataTypePtr data_type);

}
