#include <Databases/SQLite/fetchSQLiteTableStructure.h>

#if USE_SQLITE

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
    extern const int SQLITE_ENGINE_ERROR;
}

static DataTypePtr convertSQLiteDataType(String type)
{
    DataTypePtr res;
    type = Poco::toLower(type);

    if (type == "tinyint")
        res = std::make_shared<DataTypeInt8>();
    else if (type == "smallint")
        res = std::make_shared<DataTypeInt16>();
    else if (type.starts_with("int") || type == "mediumint")
        res = std::make_shared<DataTypeInt32>();
    else if (type == "bigint")
        res = std::make_shared<DataTypeInt64>();
    else if (type == "float")
        res = std::make_shared<DataTypeFloat32>();
    else if (type.starts_with("double") || type == "real")
        res = std::make_shared<DataTypeFloat64>();
    else
        res = std::make_shared<DataTypeString>(); // No decimal when fetching data through API

    return res;
}


std::shared_ptr<NamesAndTypesList> fetchSQLiteTableStructure(sqlite3 * connection, const String & sqlite_table_name)
{
    auto columns = NamesAndTypesList();
    auto query = fmt::format("pragma table_info({});", quoteString(sqlite_table_name));

    auto callback_get_data = [](void * res, int col_num, char ** data_by_col, char ** col_names) -> int
    {
        NameAndTypePair name_and_type;
        bool is_nullable = false;

        for (int i = 0; i < col_num; ++i)
        {
            if (col_names[i] == "name"sv)
            {
                name_and_type.name = data_by_col[i];
            }
            else if (col_names[i] == "type"sv)
            {
                name_and_type.type = convertSQLiteDataType(data_by_col[i]);
            }
            else if (col_names[i] == "notnull"sv)
            {
                is_nullable = (data_by_col[i][0] == '0');
            }
        }

        if (is_nullable)
            name_and_type.type = std::make_shared<DataTypeNullable>(name_and_type.type);

        static_cast<NamesAndTypesList *>(res)->push_back(name_and_type);

        return 0;
    };

    char * err_message = nullptr;
    int status = sqlite3_exec(connection, query.c_str(), callback_get_data, &columns, &err_message);

    if (status != SQLITE_OK)
    {
        String err_msg(err_message);
        sqlite3_free(err_message);

        throw Exception(ErrorCodes::SQLITE_ENGINE_ERROR,
                        "Failed to fetch SQLite data. Status: {}. Message: {}",
                        status, err_msg);
    }

    if (columns.empty())
        return nullptr;

    return std::make_shared<NamesAndTypesList>(columns);
}

}

#endif
