#include <Databases/DuckDB/fetchDuckDBTableStructure.h>

#if USE_DUCKDB

#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <Databases/DuckDB/DuckDBUtils.h>


using namespace std::literals;

namespace DB
{

namespace ErrorCodes
{
    extern const int DUCKDB_ENGINE_ERROR;
}

static DataTypePtr convertDuckDBDataType(String type)
{
    DataTypePtr res = DataTypeFactory::instance().tryGet(type);

    type = Poco::toLower(type);

    if (type == "date")
        res = std::make_shared<DataTypeDate32>();
    else if (type.starts_with("timestamp"))
        res = std::make_shared<DataTypeDateTime64>(6);
    else if (!res || type == "time" || type == "bit")
        res = std::make_shared<DataTypeString>();

    return res;
}

std::shared_ptr<NamesAndTypesList> fetchDuckDBTableStructure(duckdb::DuckDB & duckdb_instance, const String & duckdb_table_name)
{
    auto columns = NamesAndTypesList();
    auto query = fmt::format("SELECT * FROM information_schema.columns where table_name={};", quoteStringDuckDB(duckdb_table_name));

    duckdb::Connection con(duckdb_instance);
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
                if (result->names[column_index] == "column_name")
                {
                    name_and_type.name = chunk->GetValue(column_index, idx).GetValue<std::string>();
                }
                else if (result->names[column_index] == "data_type")
                {
                    name_and_type.type = convertDuckDBDataType(chunk->GetValue(column_index, idx).GetValue<std::string>());
                }
                else if (result->names[column_index] == "is_nullable")
                {
                    is_nullable = chunk->GetValue(column_index, idx).GetValue<std::string>() == "YES";
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
