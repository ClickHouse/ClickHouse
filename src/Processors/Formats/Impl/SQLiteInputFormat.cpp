#include "config.h"

#if USE_SQLITE

#    include <Databases/SQLite/fetchSQLiteTableStructure.h>
#    include <Formats/FormatFactory.h>
#    include <IO/WriteBufferFromString.h>
#    include <IO/WriteHelpers.h>
#    include <Processors/Formats/IInputFormat.h>
#    include <Processors/Formats/IRowInputFormat.h>
#    include <Processors/Formats/ISchemaReader.h>
#    include <Processors/Sources/SQLiteStatementReader.h>
#    include <Processors/Formats/Impl/SQLiteCommon.h>
#    include <Common/quoteString.h>

namespace DB
{

namespace ErrorCodes
{
extern const int SQLITE_ENGINE_ERROR;
}

namespace
{

using namespace SQLiteFormatImpl;

String makeSelectQuery(const Block & header, const String & table_name)
{
    WriteBufferFromOwnString query;
    writeCString("SELECT ", query);

    for (size_t i = 0; i != header.columns(); ++i)
    {
        if (i)
            writeCString(", ", query);
        writeString(doubleQuoteString(header.getByPosition(i).name), query);
    }

    writeCString(" FROM ", query);
    writeString(doubleQuoteString(table_name), query);
    return query.str();
}

class SQLiteInputFormat final : public IInputFormat
{
public:
    SQLiteInputFormat(ReadBuffer & in_, SharedHeader header_, const FormatSettings & settings_, UInt64 max_block_size_)
        : IInputFormat(header_, &in_)
        , header(std::move(header_))
        , settings(settings_)
        , max_block_size(max_block_size_)
        , statement_reader(*header, settings, SQLiteStatementReader::ValueReadMode::Text)
    {
    }

    String getName() const override { return "SQLite"; }

    Chunk read() override
    {
        if (sqlite_finished)
            return {};

        if (!initialized)
            initialize();

        bool finished = false;
        auto chunk = statement_reader.readChunk(sqlite_db.get(), statement.get(), max_block_size, finished);
        if (finished)
        {
            statement.reset();
            sqlite_finished = true;
        }

        return chunk;
    }

private:
    void initialize()
    {
        sqlite_db = copyToTemporaryFileAndOpenSQLiteDatabase(*in, temporary_file);
        statement = prepareSQLiteStatement(sqlite_db.get(), makeSelectQuery(*header, settings.sqlite.input_table_name));
        initialized = true;
    }

    SharedHeader header;
    FormatSettings settings;
    UInt64 max_block_size;
    SQLiteStatementReader statement_reader;
    SQLiteTemporaryFile temporary_file;
    SQLitePtr sqlite_db{nullptr, sqlite3_close};
    SQLiteStatementPtr statement{nullptr, sqlite3_finalize};
    bool initialized = false;
    bool sqlite_finished = false;
};

class SQLiteSchemaReader final : public ISchemaReader
{
public:
    SQLiteSchemaReader(ReadBuffer & in_, const FormatSettings & settings_)
        : ISchemaReader(in_)
        , settings(settings_)
    {
    }

    NamesAndTypesList readSchema() override
    {
        auto db = copyToTemporaryFileAndOpenSQLiteDatabase(in, temporary_file);
        auto columns = fetchSQLiteTableStructure(db.get(), settings.sqlite.input_table_name);

        if (!columns)
            throw Exception(
                ErrorCodes::SQLITE_ENGINE_ERROR, "Cannot fetch table structure for SQLite table {}", settings.sqlite.input_table_name);

        return *columns;
    }

private:
    FormatSettings settings;
    SQLiteTemporaryFile temporary_file;
};

}

void registerInputFormatSQLite(FormatFactory & factory)
{
    factory.registerInputFormat(
        "SQLite",
        [](ReadBuffer & buf, const Block & sample, const RowInputFormatParams & params, const FormatSettings & settings)
        { return std::make_shared<SQLiteInputFormat>(buf, std::make_shared<const Block>(sample), settings, params.max_block_size_rows); });

    factory.markFormatSupportsSubsetOfColumns("SQLite");
    factory.registerFileExtension("sqlite", "SQLite");
    factory.registerFileExtension("sqlite3", "SQLite");
}

void registerSQLiteSchemaReader(FormatFactory & factory)
{
    factory.registerSchemaReader(
        "SQLite", [](ReadBuffer & buf, const FormatSettings & settings) { return std::make_shared<SQLiteSchemaReader>(buf, settings); });
}

}

#endif
