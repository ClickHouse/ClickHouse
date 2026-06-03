#include <Processors/Formats/Impl/SQLiteOutputFormat.h>

#if USE_SQLITE

#include <sys/stat.h>

#include <Columns/IColumn.h>
#include <Common/Exception.h>
#include <Common/ErrnoException.h>
#include <Formats/FormatFactory.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>

#include <sqlite3.h>

namespace DB
{

namespace ErrorCodes
{
extern const int SQLITE_ENGINE_ERROR;
extern const int CANNOT_FSTAT;
extern const int NOT_IMPLEMENTED;
}

namespace
{

/// Quotes an SQLite identifier (a column or table name).
String quoteSQLiteIdentifier(const String & name)
{
    String result = "\"";
    for (char c : name)
    {
        if (c == '"')
            result += "\"\"";
        else
            result += c;
    }
    result += "\"";
    return result;
}

}

SQLiteOutputFormat::SQLiteOutputFormat(WriteBuffer & out_, SharedHeader header_, const FormatSettings & settings_)
    : IOutputFormat(header_, out_), format_settings(settings_)
{
}

void SQLiteOutputFormat::writePrefix()
{
    if (db)
        return;

    /// SQLite operates on a file on disk, so we can only write to a real (seekable) file,
    /// not to a pipe, socket, terminal or another in-memory write buffer.
    auto * fd_buffer = dynamic_cast<WriteBufferFromFileDescriptor *>(&out);
    if (!fd_buffer)
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "SQLite output format only supports writing to a file, but the output is not a file descriptor");

    int fd = fd_buffer->getFD();

    struct stat file_stat;
    if (fstat(fd, &file_stat) != 0)
        throw ErrnoException(ErrorCodes::CANNOT_FSTAT, "Cannot fstat the output file of the SQLite output format");

    if (!S_ISREG(file_stat.st_mode))
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "SQLite output format only supports writing to a regular file, not to pipes, sockets or terminals");

    String uri = fmt::format("file:///dev/fd/{}", fd);

    sqlite3 * db_ptr = nullptr;
    int status = sqlite3_open_v2(uri.c_str(), &db_ptr, SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, nullptr);
    if (status != SQLITE_OK)
        throw Exception::createDeprecated(
            fmt::format("Cannot open SQLite database. Error status: {}. Message: {}", status, sqlite3_errstr(status)),
            ErrorCodes::SQLITE_ENGINE_ERROR);
    db.reset(db_ptr, sqlite3_close_v2);

    const auto & header = getPort(PortKind::Main).getHeader();
    auto names_and_types = header.getNamesAndTypes();

    String columns_definition;
    String placeholders;
    for (size_t i = 0; i < names_and_types.size(); ++i)
    {
        if (i != 0)
        {
            columns_definition += ", ";
            placeholders += ", ";
        }
        /// Declare the column without a type. SQLite is dynamically typed, and ClickHouse type
        /// names (such as `Nullable(UInt64)`) are not valid SQLite type names anyway. Without a
        /// declared type the column has no affinity, so the values we bind as text are stored as is.
        columns_definition += quoteSQLiteIdentifier(names_and_types[i].name);
        placeholders += "?";
        serializations.emplace_back(names_and_types[i].type->getDefaultSerialization());
    }

    String create_query = fmt::format("CREATE TABLE IF NOT EXISTS result({})", columns_definition);
    status = sqlite3_exec(db.get(), create_query.c_str(), nullptr, nullptr, nullptr);
    if (status != SQLITE_OK)
        throw Exception::createDeprecated(
            fmt::format("Cannot create SQLite table. Error status: {}. Message: {}", status, sqlite3_errmsg(db.get())),
            ErrorCodes::SQLITE_ENGINE_ERROR);

    /// Wrap all inserts in a single transaction, otherwise every INSERT is committed separately and the writing is very slow.
    status = sqlite3_exec(db.get(), "BEGIN TRANSACTION", nullptr, nullptr, nullptr);
    if (status != SQLITE_OK)
        throw Exception::createDeprecated(
            fmt::format("Cannot start SQLite transaction. Error status: {}. Message: {}", status, sqlite3_errmsg(db.get())),
            ErrorCodes::SQLITE_ENGINE_ERROR);

    String insert_query = fmt::format("INSERT INTO result VALUES ({})", placeholders);
    sqlite3_stmt * stmt_ptr = nullptr;
    status = sqlite3_prepare_v2(db.get(), insert_query.c_str(), static_cast<int>(insert_query.size()), &stmt_ptr, nullptr);
    if (status != SQLITE_OK)
        throw Exception::createDeprecated(
            fmt::format("Cannot prepare SQLite insert statement. Error status: {}. Message: {}", status, sqlite3_errmsg(db.get())),
            ErrorCodes::SQLITE_ENGINE_ERROR);
    insert_stmt.reset(stmt_ptr, sqlite3_finalize);
}

void SQLiteOutputFormat::consume(Chunk chunk)
{
    const auto & columns = chunk.getColumns();
    size_t num_rows = chunk.getNumRows();
    size_t num_columns = chunk.getNumColumns();

    for (size_t row = 0; row < num_rows; ++row)
    {
        for (size_t col = 0; col < num_columns; ++col)
        {
            int index = static_cast<int>(col) + 1;
            if (columns[col]->isNullAt(row))
            {
                sqlite3_bind_null(insert_stmt.get(), index);
            }
            else
            {
                WriteBufferFromOwnString text_buffer;
                serializations[col]->serializeText(*columns[col], row, text_buffer, format_settings);
                const String & value = text_buffer.str();
                /// SQLITE_TRANSIENT makes SQLite copy the value before sqlite3_step returns.
                sqlite3_bind_text(insert_stmt.get(), index, value.data(), static_cast<int>(value.size()), SQLITE_TRANSIENT);
            }
        }

        int status = sqlite3_step(insert_stmt.get());
        if (status != SQLITE_DONE)
            throw Exception::createDeprecated(
                fmt::format("Cannot insert into SQLite table. Error status: {}. Message: {}", status, sqlite3_errmsg(db.get())),
                ErrorCodes::SQLITE_ENGINE_ERROR);

        sqlite3_reset(insert_stmt.get());
        sqlite3_clear_bindings(insert_stmt.get());
    }
}

void SQLiteOutputFormat::writeSuffix()
{
    if (!db)
        return;

    int status = sqlite3_exec(db.get(), "COMMIT", nullptr, nullptr, nullptr);
    if (status != SQLITE_OK)
        throw Exception::createDeprecated(
            fmt::format("Cannot commit SQLite transaction. Error status: {}. Message: {}", status, sqlite3_errmsg(db.get())),
            ErrorCodes::SQLITE_ENGINE_ERROR);
}

void registerOutputFormatSQLite(FormatFactory & factory);
void registerOutputFormatSQLite(FormatFactory & factory)
{
    factory.registerOutputFormat(
        "SQLite",
        [](WriteBuffer & buf, const Block & sample, const FormatSettings & settings, FormatFilterInfoPtr)
        { return std::make_shared<SQLiteOutputFormat>(buf, std::make_shared<const Block>(sample), settings); });
}

}

#endif
