#include <Databases/SQLite/fetchSQLiteTableStructure.h>

#if USE_SQLITE

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


namespace DB
{

namespace ErrorCodes
{
    extern const int SQLITE_ENGINE_ERROR;
}

DataTypePtr convertSQLiteDataType(String type)
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


std::optional<ColumnsDescription> fetchSQLiteTableStructure(sqlite3 * connection, const String & sqlite_table_name)
{
    ColumnsDescription columns;

    /// Use `table_xinfo` rather than `table_info` so that generated columns (which `SELECT *` returns) are
    /// included; `table_info` omits them, which would silently drop visible columns from the table structure.
    ///
    /// The table name is passed as a bound parameter of the `pragma_table_xinfo` table-valued function
    /// instead of being re-serialized into the SQL text. SQLite string literals have no escape sequences
    /// (only an embedded quote is doubled, a backslash or a control character stays literal), so any
    /// backslash-style textual escaping of the name would make the lookup miss a table whose name contains
    /// e.g. a newline or a tab; a bound parameter with an explicit length hands the name over byte-faithfully.
    static constexpr std::string_view query = R"(SELECT "name", "type", "notnull", "hidden" FROM pragma_table_xinfo(?))";

    sqlite3_stmt * compiled_stmt = nullptr;
    int status = sqlite3_prepare_v2(connection, query.data(), static_cast<int>(query.size()), &compiled_stmt, nullptr);
    if (status != SQLITE_OK)
        throw Exception(ErrorCodes::SQLITE_ENGINE_ERROR,
                        "Failed to prepare SQLite table structure query. Status: {}. Message: {}",
                        status, sqlite3_errmsg(connection));

    std::unique_ptr<sqlite3_stmt, decltype(&sqlite3_finalize)> statement(compiled_stmt, sqlite3_finalize);

    status = sqlite3_bind_text64(compiled_stmt, 1, sqlite_table_name.data(), sqlite_table_name.size(), SQLITE_STATIC, SQLITE_UTF8);
    if (status != SQLITE_OK)
        throw Exception(ErrorCodes::SQLITE_ENGINE_ERROR,
                        "Failed to bind the table name for the SQLite table structure query. Status: {}. Message: {}",
                        status, sqlite3_errmsg(connection));

    while (true)
    {
        status = sqlite3_step(compiled_stmt);
        if (status == SQLITE_DONE)
            break;

        if (status != SQLITE_ROW)
            throw Exception(ErrorCodes::SQLITE_ENGINE_ERROR,
                            "Failed to fetch SQLite data. Status: {}. Message: {}",
                            status, sqlite3_errmsg(connection));

        const auto * name_data = reinterpret_cast<const char *>(sqlite3_column_text(compiled_stmt, 0));
        String name = name_data ? String(name_data, sqlite3_column_bytes(compiled_stmt, 0)) : String{};

        const auto * type_data = reinterpret_cast<const char *>(sqlite3_column_text(compiled_stmt, 1));
        String type_name = type_data ? String(type_data, sqlite3_column_bytes(compiled_stmt, 1)) : String{};

        bool is_nullable = sqlite3_column_int(compiled_stmt, 2) == 0;
        int hidden = sqlite3_column_int(compiled_stmt, 3);

        /// `table_xinfo` reports hidden = 1 for columns that `SELECT *` does not return (e.g. the hidden
        /// columns of virtual tables); skip them. Generated columns use hidden = 2 (VIRTUAL) or 3 (STORED)
        /// and are returned by `SELECT *`, so they stay in the structure to keep them readable.
        if (hidden == 1)
            continue;

        DataTypePtr type = convertSQLiteDataType(type_name);
        if (is_nullable)
            type = std::make_shared<DataTypeNullable>(type);

        ColumnDescription column(std::move(name), std::move(type));

        /// SQLite computes generated columns itself and rejects explicit writes into them. Mark them
        /// `MATERIALIZED` so ClickHouse keeps them readable but non-insertable: an explicit insert into a
        /// generated column is rejected, and an insert without a column list targets only the base columns.
        if (hidden == 2 || hidden == 3)
            column.default_desc.kind = ColumnDefaultKind::Materialized;

        columns.add(std::move(column));
    }

    if (columns.empty())
        return std::nullopt;

    return columns;
}

}

#endif
