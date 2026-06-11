#include <Processors/Formats/Impl/SQLiteInputFormat.h>

#if USE_SQLITE

#    include <Columns/IColumn.h>
#    include <Columns/ColumnNullable.h>
#    include <Core/Block.h>
#    include <DataTypes/DataTypeNullable.h>
#    include <Formats/FormatFactory.h>
#    include <IO/ReadBufferFromMemory.h>
#    include <IO/ReadBufferFromString.h>
#    include <IO/SeekableReadBuffer.h>
#    include <IO/WithFileSize.h>
#    include <IO/WriteBufferFromString.h>
#    include <IO/copyData.h>
#    include <Processors/Formats/Impl/SQLiteInputVFS.h>
#    include <Common/Exception.h>
#    include <Common/assert_cast.h>

#    include <sqlite3.h>

#    include <optional>
#    include <utility>

namespace DB
{

namespace ErrorCodes
{
extern const int SQLITE_ENGINE_ERROR;
extern const int UNKNOWN_TABLE;
extern const int INCORRECT_NUMBER_OF_COLUMNS;
extern const int INCORRECT_DATA;
}

SQLiteInputFormat::SQLiteInputFormat(
    ReadBuffer & in_, SharedHeader header_, Params params_, const FormatSettings & format_settings_)
    : IRowInputFormat(header_, in_, params_), table_name(format_settings_.sqlite.table_name), format_settings(format_settings_)
{
    data_types = header_->getDataTypes();
    nested_types.resize(data_types.size());
    nested_serializations.resize(data_types.size());
    for (size_t i = 0; i < data_types.size(); ++i)
    {
        if (isNullableOrLowCardinalityNullable(data_types[i]))
        {
            nested_types[i] = removeNullableOrLowCardinalityNullable(data_types[i]);
            nested_serializations[i] = nested_types[i]->getDefaultSerialization();
        }
    }

    initSQLiteReadVFS();
}

void SQLiteInputFormat::rethrowSavedReadException()
{
    if (read_source.exception)
        std::rethrow_exception(std::exchange(read_source.exception, nullptr));
}

void SQLiteInputFormat::prepareReader()
{
    /// SQLite reads its database with random access. If seeks are allowed by the
    /// input_format_allow_seeks setting and the input buffer is seekable and its size is known,
    /// read directly from it; otherwise (a pipe, a stream of unknown length, ...) load the whole
    /// database into memory and serve random access from there.
    auto * seekable = dynamic_cast<SeekableReadBuffer *>(in);
    std::optional<size_t> file_size = tryGetFileSizeFromReadBuffer(*in);

    if (seekable == nullptr || !format_settings.seekable_read || !seekable->checkIfActuallySeekable() || !file_size)
    {
        String content;
        {
            WriteBufferFromString wb(content);
            copyData(*in, wb);
            wb.finalize();
        }
        file_size = content.size();
        owned_buffer = std::make_unique<ReadBufferFromOwnString>(std::move(content));
        seekable = owned_buffer.get();
    }

    read_source = SQLiteReadSource{seekable, *file_size, nullptr};
    std::string uri = encodeSQLiteVFSFileName(&read_source);

    sqlite3 * db_ptr = nullptr;
    int status = sqlite3_open_v2(uri.c_str(), &db_ptr, SQLITE_OPEN_READONLY | SQLITE_OPEN_URI, sqlite_read_vfs_name);
    if (status != SQLITE_OK)
    {
        rethrowSavedReadException();
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
        rethrowSavedReadException();
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

    std::string select_query = fmt::format("SELECT * FROM {};", quoteSQLiteIdentifier(table_name));
    sqlite3_stmt * stmt_ptr = nullptr;
    int status = sqlite3_prepare_v2(db.get(), select_query.c_str(), static_cast<int>(select_query.size()), &stmt_ptr, nullptr);
    if (status != SQLITE_OK)
    {
        rethrowSavedReadException();
        throw Exception::createDeprecated(
            fmt::format("Cannot read from SQLite table {}. Error status: {}. Message: {}", table_name, status, sqlite3_errmsg(db.get())),
            ErrorCodes::SQLITE_ENGINE_ERROR);
    }
    stmt.reset(stmt_ptr, sqlite3_finalize);

    /// The structure of the data must be provided explicitly, and the SQLite table is read by
    /// position. If the widths do not match, `SELECT *` would otherwise either silently drop
    /// extra SQLite columns or read out-of-range indexes for the missing ones, so reject the
    /// mismatch here.
    size_t sqlite_columns = static_cast<size_t>(sqlite3_column_count(stmt.get()));
    if (sqlite_columns != serializations.size())
        throw Exception(
            ErrorCodes::INCORRECT_NUMBER_OF_COLUMNS,
            "The SQLite table {} has {} column(s), but {} column(s) were provided in the structure",
            table_name, sqlite_columns, serializations.size());
}

bool SQLiteInputFormat::readRow(MutableColumns & columns, RowReadExtension & ext)
{
    if (!continue_read)
        return false;

    auto errcode = sqlite3_step(stmt.get());

    if (errcode == SQLITE_DONE)
    {
        continue_read = false;
        return false;
    }

    /// Only SQLITE_DONE marks the clean end of the result set. Any other status (a corrupt or
    /// truncated database, a VFS I/O error, SQLITE_BUSY, etc.) is a real error and must not be
    /// turned into a successful EOF that returns a partial result set.
    if (errcode != SQLITE_ROW)
    {
        rethrowSavedReadException();
        throw Exception::createDeprecated(
            fmt::format("Cannot read from SQLite table {}. Error status: {}. Message: {}",
                table_name, errcode, sqlite3_errmsg(db.get())),
            ErrorCodes::SQLITE_ENGINE_ERROR);
    }

    ext.read_columns.assign(serializations.size(), true);

    for (size_t i = 0; i < serializations.size(); i++)
    {
        int column = static_cast<int>(i);
        const bool is_nullable = data_types[i]->isNullable();
        const bool is_lc_nullable = data_types[i]->isLowCardinalityNullable();

        if (sqlite3_column_type(stmt.get(), column) == SQLITE_NULL)
        {
            if (is_nullable || is_lc_nullable)
            {
                /// Inserts SQL NULL: for both Nullable and LowCardinality(Nullable) columns
                /// the default value is NULL.
                columns[i]->insertDefault();
            }
            else if (format_settings.null_as_default)
            {
                /// Insert the type default and mark the column as not read in this row, so
                /// that AddingDefaultsTransform can compute the table DEFAULT expression
                /// for it if there is one.
                columns[i]->insertDefault();
                ext.read_columns[i] = false;
            }
            else
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "Cannot insert SQLite NULL into non-nullable column {} of type {}. "
                    "Enable setting input_format_null_as_default to insert a default value instead",
                    getPort().getHeader().getByPosition(i).name, data_types[i]->getName());
            continue;
        }

        const auto * value = reinterpret_cast<const char *>(sqlite3_column_text(stmt.get(), column));
        size_t value_len = sqlite3_column_bytes(stmt.get(), column);
        ReadBufferFromMemory string_buffer(value, value_len);

        /// The value is known to be non-NULL (checked above), so deserialize it as the
        /// underlying (non-nullable) value. Otherwise a SQLite text value equal to the textual
        /// null marker (e.g. 'NULL' or 'ᴺᵁᴸᴸ') would be mistaken for SQL NULL.
        if (is_nullable)
        {
            auto & nullable_column = assert_cast<ColumnNullable &>(*columns[i]);
            nested_serializations[i]->deserializeWholeText(nullable_column.getNestedColumn(), string_buffer, format_settings);
            nullable_column.getNullMapColumn().insertValue(0);
        }
        else if (is_lc_nullable)
        {
            /// Deserialize as LowCardinality of the non-nullable type, then insert the value
            /// into the LowCardinality(Nullable) column.
            auto tmp_column = nested_types[i]->createColumn();
            nested_serializations[i]->deserializeWholeText(*tmp_column, string_buffer, format_settings);
            columns[i]->insertFrom(*tmp_column, 0);
        }
        else
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
