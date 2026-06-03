#include <Processors/Formats/Impl/SQLiteInputFormat.h>

#if USE_SQLITE

#    include <Columns/IColumn.h>
#    include <Core/Block.h>
#    include <Formats/FormatFactory.h>
#    include <IO/ReadBufferFromMemory.h>
#    include <Processors/Formats/Impl/ArrowBufferedStreams.h>
#    include <Processors/Formats/Impl/SQLiteInputVFS.h>
#    include <arrow/result.h>
#    include <Common/Exception.h>

#    include <sqlite3.h>

namespace DB
{

namespace ErrorCodes
{
extern const int SQLITE_ENGINE_ERROR;
extern const int UNKNOWN_TABLE;
}

SQLiteInputFormat::SQLiteInputFormat(
    ReadBuffer & in_, SharedHeader header_, Params params_, const FormatSettings & format_settings_)
    : IRowInputFormat(header_, in_, params_), table_name(format_settings_.sqlite.table_name), format_settings(format_settings_)
{
    initSQLiteReadVFS();
}

void SQLiteInputFormat::prepareReader()
{
    std::atomic<int> is_stopped = 0;
    file_reader = asArrowFile(*in, format_settings, is_stopped, "sqlite3", "");

    std::string uri = encodeSQLiteVFSFileName(file_reader.get());

    sqlite3 * db_ptr = nullptr;
    int status = sqlite3_open_v2(uri.c_str(), &db_ptr, SQLITE_OPEN_READONLY | SQLITE_OPEN_URI, sqlite_read_vfs_name);
    if (status != SQLITE_OK)
    {
        throw Exception::createDeprecated(
            fmt::format("Cannot open sqlite database. Error status: {}. Message: {}", status, sqlite3_errstr(status)),
            ErrorCodes::SQLITE_ENGINE_ERROR);
    }
    db.reset(db_ptr, sqlite3_close_v2);
}

std::vector<String> SQLiteInputFormat::getTablesNames()
{
    std::vector<String> tables_names;
    std::string query = "SELECT name FROM sqlite_master "
                        "WHERE type = 'table' AND name NOT LIKE 'sqlite_%'";

    auto callback_get_data = [](void * res, int col_num, char ** data_by_col, char ** /* col_names */) -> int
    {
        for (int i = 0; i < col_num; ++i)
            static_cast<std::vector<String> *>(res)->push_back(String(data_by_col[i]));
        return 0;
    };


    int status = sqlite3_exec(db.get(), query.c_str(), callback_get_data, &tables_names, nullptr);

    if (status != SQLITE_OK)
    {
        throw Exception::createDeprecated(
            fmt::format("Failed to fetch SQLite tables names. Error status: {}. Message: {}", status, sqlite3_errstr(status)),
            ErrorCodes::SQLITE_ENGINE_ERROR);
    }

    return tables_names;
}

void SQLiteInputFormat::readPrefix()
{
    prepareReader();

    auto tables_names = getTablesNames();
    if (table_name.empty())
    {
        if (tables_names.empty())
            throw Exception(ErrorCodes::UNKNOWN_TABLE, "Can't get any table");
        table_name = tables_names[0];
    }
    else
    {
        if (std::find(tables_names.begin(), tables_names.end(), table_name) == tables_names.end())
            throw Exception::createDeprecated(fmt::format("Failed to find SQLite {} table", table_name), ErrorCodes::UNKNOWN_TABLE);
    }

    std::string select_query = fmt::format("SELECT * FROM \"{}\";", table_name);
    sqlite3_stmt * stmt_ptr = nullptr;
    int status = sqlite3_prepare_v2(db.get(), select_query.c_str(), static_cast<int>(select_query.size()), &stmt_ptr, nullptr);
    if (status != SQLITE_OK)
        throw Exception::createDeprecated(
            fmt::format("Cannot read from SQLite table {}. Error status: {}. Message: {}", table_name, status, sqlite3_errmsg(db.get())),
            ErrorCodes::SQLITE_ENGINE_ERROR);
    stmt.reset(stmt_ptr, sqlite3_finalize);
}

bool SQLiteInputFormat::readRow(MutableColumns & columns, RowReadExtension & /*ext*/)
{
    if (!continue_read)
        return false;

    auto errcode = sqlite3_step(stmt.get());

    if (SQLITE_ROW != errcode)
    {
        continue_read = false;
        return false;
    }

    for (size_t i = 0; i < serializations.size(); i++)
    {
        int column = static_cast<int>(i);
        if (sqlite3_column_type(stmt.get(), column) == SQLITE_NULL)
        {
            /// For a Nullable column this inserts NULL, otherwise the type default.
            columns[i]->insertDefault();
            continue;
        }

        const auto * value = reinterpret_cast<const char *>(sqlite3_column_text(stmt.get(), column));
        size_t value_len = sqlite3_column_bytes(stmt.get(), column);
        ReadBufferFromMemory string_buffer(value, value_len);

        serializations[i]->deserializeWholeText(*columns[i], string_buffer, format_settings);
    }

    return true;
}


void registerInputFormatSQLite(FormatFactory & factory);
void registerInputFormatSQLite(FormatFactory & factory)
{
    factory.registerInputFormat(
        "SQLite",
        [](ReadBuffer & buf, const Block & header, const RowInputFormatParams & params, const FormatSettings & settings)
        { return std::make_shared<SQLiteInputFormat>(buf, std::make_shared<const Block>(header), params, settings); });
}

}

#endif
