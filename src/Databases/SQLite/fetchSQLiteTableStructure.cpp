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

    /// The SQLite columns get the INTEGER affinity if the type name contains "int". This means variable-length integers up to 8 bytes. The bit width is not really enforced even
    /// in a STRICT table, so in general we should treat these columns as Int64. Besides that, we allow some common fixed-width int specifiers for applications to select a
    /// particular width, even though it's not enforced in any way by SQLite itself.
    /// Docs: https://www.sqlite.org/datatype3.html
    /// The most insane quote from there: Note that a declared type of "FLOATING POINT" would give INTEGER affinity, not REAL affinity, due to the "INT" at the end of "POINT".
    if (type.find("int") != std::string::npos)
        res = std::make_shared<DataTypeInt64>();
    else if (type == "float" || type.starts_with("double") || type == "real")
        res = std::make_shared<DataTypeFloat64>();
    else
        res = std::make_shared<DataTypeString>(); // No decimal when fetching data through API

    return res;
}


namespace
{

struct FetchTableStructureContext
{
    NamesAndTypesList columns;
    std::unordered_set<String> * generated_columns = nullptr;
};

}

std::shared_ptr<NamesAndTypesList> fetchSQLiteTableStructure(
    sqlite3 * connection,
    const String & sqlite_table_name,
    std::unordered_set<String> * out_generated_columns)
{
    FetchTableStructureContext context;
    context.generated_columns = out_generated_columns;

    /// Use `table_xinfo` rather than `table_info` so that generated columns (which `SELECT *` returns) are
    /// included; `table_info` omits them, which would silently drop visible columns from the table structure.
    auto query = fmt::format("pragma table_xinfo({});", quoteStringSQLite(sqlite_table_name));

    auto callback_get_data = [](void * res, int col_num, char ** data_by_col, char ** col_names) -> int
    {
        auto & ctx = *static_cast<FetchTableStructureContext *>(res);

        NameAndTypePair name_and_type;
        bool is_nullable = false;
        int hidden = 0;

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
            else if (col_names[i] == "hidden"sv)
            {
                hidden = data_by_col[i][0] - '0';
            }
        }

        /// `table_xinfo` reports hidden = 1 for columns that `SELECT *` does not return (e.g. the
        /// hidden columns of virtual tables); skip them. Generated columns use hidden = 2 (VIRTUAL) or
        /// 3 (STORED) and are returned by `SELECT *`, so they stay in the structure to keep them
        /// readable, but they are recorded as generated so the write path can omit them (SQLite rejects
        /// explicit inserts into generated columns).
        if (hidden == 1)
            return 0;

        if (is_nullable)
            name_and_type.type = std::make_shared<DataTypeNullable>(name_and_type.type);

        if ((hidden == 2 || hidden == 3) && ctx.generated_columns)
            ctx.generated_columns->insert(name_and_type.name);

        ctx.columns.push_back(name_and_type);

        return 0;
    };

    char * err_message = nullptr;
    int status = sqlite3_exec(connection, query.c_str(), callback_get_data, &context, &err_message);

    if (status != SQLITE_OK)
    {
        String err_msg(err_message ? err_message : "unknown error");
        sqlite3_free(err_message);

        throw Exception(ErrorCodes::SQLITE_ENGINE_ERROR,
                        "Failed to fetch SQLite data. Status: {}. Message: {}",
                        status, err_msg);
    }

    if (context.columns.empty())
        return nullptr;

    return std::make_shared<NamesAndTypesList>(context.columns);
}

}

#endif
