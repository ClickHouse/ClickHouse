#include <Databases/SQLite/fetchSQLiteTableStructure.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>

namespace DB
{
static DataTypePtr convertSQLiteDataType(std::string type /* , bool is_nullable */)
{
    DataTypePtr res;

    std::transform(std::begin(type), std::end(type), std::begin(type), tolower);

    // https://www.sqlite.org/datatype3.html#determination_of_column_affinity
    if (type.find("int") != std::string::npos)
        res = std::make_shared<DataTypeInt64>();
    else if (
        type.find("char") != std::string::npos || type.find("clob") != std::string::npos || type.find("text") != std::string::npos
        || type.empty() || type.find("blob") != std::string::npos)
        res = std::make_shared<DataTypeString>();
    else if (type.find("real") != std::string::npos || type.find("floa") != std::string::npos || type.find("doub") != std::string::npos)
        res = std::make_shared<DataTypeFloat64>();
    else
        res = std::make_shared<DataTypeString>(); // No decimal when fetching data through API

    return res;
}

std::shared_ptr<NamesAndTypesList> fetchSQLiteTableStructure(sqlite3 * connection, const String & sqlite_table_name /* , bool use_nulls */)
{
    auto columns = NamesAndTypesList();

    std::string query = fmt::format("pragma table_info({});", sqlite_table_name);

    auto callback_get_data = [](void * res, int col_num, char ** data_by_col, char ** col_names) -> int {
        NameAndTypePair name_and_type;

        for (int i = 0; i < col_num; ++i)
        {
            if (strcmp(col_names[i], "name") == 0)
            {
                name_and_type.name = data_by_col[i];
            }
            else if (strcmp(col_names[i], "type") == 0)
            {
                name_and_type.type = convertSQLiteDataType(data_by_col[i]);
            }
        }

        static_cast<NamesAndTypesList *>(res)->push_back(name_and_type);

        return 0;
    };

    char * err_message = nullptr;

    int status = sqlite3_exec(connection, query.c_str(), callback_get_data, &columns, &err_message);

    if (status != SQLITE_OK)
    {
        String err_msg(err_message);
        sqlite3_free(err_message);
        throw Exception(status, "SQLITE_ERR {}: {}", status, err_msg);
    }

    if (columns.empty())
        return nullptr;

    return std::make_shared<NamesAndTypesList>(columns);
}

}
