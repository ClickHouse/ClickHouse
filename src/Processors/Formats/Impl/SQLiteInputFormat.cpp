#include <Processors/Formats/Impl/SQLiteInputFormat.h>

#if USE_SQLITE

#include <IO/ReadHelpers.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/convertFieldToType.h>
#include <Parsers/TokenIterator.h>
#include <Processors/Formats/Impl/SQLiteInputFormat.h>
#include <Formats/FormatFactory.h>
#include <Formats/EscapingRuleUtils.h>
#include <Core/Block.h>
#include <base/find_symbols.h>
#include <Common/typeid_cast.h>
#include <Common/checkStackSize.h>
#include <Parsers/ASTLiteral.h>
#include <DataTypes/Serializations/SerializationNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/ObjectUtils.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/ReadBufferFromMemory.h>
#include "ArrowBufferedStreams.h"
#include <arrow/result.h>
#include <iostream>
#include <fstream>
#include <Databases/SQLite/fetchSQLiteTableStructure.h>
#include <IO/ReadBufferFromMemory.h>
#include <Processors/Formats/Impl/SQLiteInputVFS.h>
#include <sstream>

#include <sqlite3.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SQLITE_ENGINE_ERROR;
    extern const int UNKNOWN_TABLE;
}

SQLiteInputFormat::SQLiteInputFormat(
    ReadBuffer & in_,
    const Block & header_,
    const RowInputFormatParams & params_,
    const FormatSettings & format_settings_)
    : IRowInputFormat(header_, in_, params_), table_name(format_settings_.sqlite.table_name), format_settings(format_settings_)
{
    initMemVFS();
}

void SQLiteInputFormat::prepareReader() {
    std::atomic<int> is_stopped = 0;
    file_reader = asArrowFile(*in, format_settings, is_stopped, "sqlite3", "");
    //file_reader = asArrowFile(*in, format_settings, is_stopped, "sqlite3", "SQLite format 3");

    std::ostringstream ss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
	ss << file_reader.get();
	std::string uri = ss.   str();

    sqlite3 * db_ptr = nullptr;
    int status = sqlite3_open_v2(uri.c_str(), &db_ptr, SQLITE_OPEN_READONLY | SQLITE_OPEN_URI, "ch_read_vfs");
    if (status != SQLITE_OK) {
        throw Exception::createDeprecated(fmt::format("Cannot open sqlite database. Error status: {}. Message: {}",
                                       status, sqlite3_errstr(status)), ErrorCodes::SQLITE_ENGINE_ERROR);
    }
    db.reset(db_ptr, sqlite3_close_v2);
}

std::vector<String> SQLiteInputFormat::getTablesNames() {
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
        throw Exception::createDeprecated(fmt::format("Failed to fetch SQLite tables names. Error status: {}. Message: {}",
                                       status, sqlite3_errstr(status)), ErrorCodes::SQLITE_ENGINE_ERROR);
    }

    return tables_names;
}

void SQLiteInputFormat::readPrefix()
{
    prepareReader();

    auto tables_names = getTablesNames();
    if (table_name.empty()) {
        if (tables_names.empty()) {
            throw Exception(ErrorCodes::UNKNOWN_TABLE, "Can't get any table");
        }
        table_name = tables_names[0];
    } else {
        if (std::find(tables_names.begin(), tables_names.end(), table_name) == tables_names.end()) {
            throw Exception::createDeprecated(fmt::format("Failed to find SQLite {} table", table_name), ErrorCodes::UNKNOWN_TABLE);
        }
    }

    /*
    auto names_and_types = fetchSQLiteTableStructure(db.get(), table_name);

    serializations.resize(names_and_types->getTypes().size());
    for (size_t i = 0; i < serializations.size(); i++) {
        serializations[i] = names_and_types->getTypes()[i]->getDefaultSerialization();
    }
    */

    std::string select_query = fmt::format("SELECT * FROM {};", table_name);
    sqlite3_stmt * stmt_ptr = nullptr;
    sqlite3_prepare( db.get(), select_query.c_str(), static_cast<int>(select_query.size()), &stmt_ptr, nullptr);
    stmt.reset(stmt_ptr, sqlite3_finalize);
}

bool SQLiteInputFormat::readRow(MutableColumns & columns, RowReadExtension & /*ext*/)
{
    if (!continue_read) {
        return false;
    }

    auto errcode = sqlite3_step(stmt.get());

    if ( SQLITE_ROW != errcode) {
        continue_read = false;
        return false;
    }

    for (size_t i = 0; i < serializations.size(); i++) {
        auto value = reinterpret_cast<const char *>(sqlite3_column_text(stmt.get(), static_cast<int>(i)));
        auto value_len = (value == nullptr) ? 0 : strlen(value);
        auto stringBuffer = ReadBufferFromMemory(value, value_len);

        serializations[i]->deserializeWholeText(*columns[i], stringBuffer, format_settings);
    }

    return true;
}



void registerInputFormatSQLite(FormatFactory & factory)
{
    factory.registerInputFormat("SQLite", [](
        ReadBuffer & buf,
        const Block & header,
        const RowInputFormatParams & params,
        const FormatSettings & settings)
    {
        return std::make_shared<SQLiteInputFormat>(buf, header, params,settings);
    });
}

}

#endif
