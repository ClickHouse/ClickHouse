#include <Databases/DuckDB/fetchDuckDBTableStructure.h>

#if USE_DUCKDB

#include <Common/quoteString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <Poco/String.h>

#include <string_view>


using namespace std::literals;

namespace DB
{

namespace ErrorCodes
{
    extern const int DUCKDB_ENGINE_ERROR;
}

static DataTypePtr convertDuckDBDataType(String type)
{
    DataTypePtr res;
    type = Poco::toLower(type);

    if (type == "tinyint")
        res = std::make_shared<DataTypeInt8>();
    else if (type == "smallint")
        res = std::make_shared<DataTypeInt16>();
    else if ((type.starts_with("int") && type != "int8") || type == "mediumint")
        res = std::make_shared<DataTypeInt32>();
    else if (type == "bigint" || type == "int8")
        res = std::make_shared<DataTypeInt64>();
    else if (type == "float")
        res = std::make_shared<DataTypeFloat32>();
    else if (type.starts_with("double") || type == "real")
        res = std::make_shared<DataTypeFloat64>();
    else
        res = std::make_shared<DataTypeString>(); // No decimal when fetching data through API

    return res;
}


std::shared_ptr<NamesAndTypesList> fetchDuckDBTableStructure(duckdb::DuckDB * duckdb_instance, const String & duckdb_table_name)
{
    auto columns = NamesAndTypesList();
    auto query = fmt::format("pragma table_info({});", quoteString(duckdb_table_name));

    duckdb::Connection con(*duckdb_instance);
    auto result = con.Query(query);

    if (result->HasError())
    {
        throw Exception(ErrorCodes::DUCKDB_ENGINE_ERROR,
                        "Cannot fetch DuckDB database tables. Error type: {}. Message: {}",
                        result->GetErrorType(), result->GetError());
    }

    auto chunk = result->Fetch();

    while (chunk)
    {
        for (size_t idx = 0; idx < chunk->size(); ++idx)
       {
            NameAndTypePair name_and_type;
            bool is_nullable = false;

            for (size_t column_index = 0; column_index < result->names.size(); ++column_index)
            {
                if (result->names[column_index] == "name")
                {
                    name_and_type.name = chunk->GetValue(column_index, idx).GetValue<std::string>();
                }
                else if (result->names[column_index] == "type")
                {
                    name_and_type.type = convertDuckDBDataType(chunk->GetValue(column_index, idx).GetValue<std::string>());
                }
                else if (result->names[column_index] == "notnull")
                {
                    is_nullable = chunk->GetValue(column_index, idx).GetValue<bool>();
                }
            }

            if (is_nullable)
                name_and_type.type = std::make_shared<DataTypeNullable>(name_and_type.type);

            columns.push_back(name_and_type);
        }

        chunk = result->Fetch();
    }

    if (columns.empty())
        return nullptr;

    return std::make_shared<NamesAndTypesList>(columns);
}

}

#endif
