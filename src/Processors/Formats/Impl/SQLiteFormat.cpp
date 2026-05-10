#include "config.h"

#if USE_SQLITE

#    include <Columns/IColumn.h>
#    include <Core/Block.h>
#    include <DataTypes/DataTypeLowCardinality.h>
#    include <DataTypes/DataTypeNullable.h>
#    include <DataTypes/IDataType.h>
#    include <DataTypes/Serializations/ISerialization.h>
#    include <Databases/SQLite/fetchSQLiteTableStructure.h>
#    include <Formats/FormatFactory.h>
#    include <IO/ReadBufferFromString.h>
#    include <IO/ReadHelpers.h>
#    include <IO/WriteBufferFromString.h>
#    include <IO/WriteHelpers.h>
#    include <Processors/Formats/IInputFormat.h>
#    include <Processors/Formats/IOutputFormat.h>
#    include <Processors/Formats/IRowInputFormat.h>
#    include <Processors/Formats/ISchemaReader.h>
#    include <base/scope_guard.h>
#    include <Common/quoteString.h>

#    include <sqlite3.h>

#    include <cstring>

namespace DB
{

namespace ErrorCodes
{
extern const int SQLITE_ENGINE_ERROR;
}

namespace
{

using SQLitePtr = std::unique_ptr<sqlite3, decltype(&sqlite3_close)>;
using SQLiteStatementPtr = std::unique_ptr<sqlite3_stmt, decltype(&sqlite3_finalize)>;

void checkSQLiteStatus(sqlite3 * db, int status, std::string_view message)
{
    if (status != SQLITE_OK && status != SQLITE_DONE && status != SQLITE_ROW)
    {
        throw Exception(
            ErrorCodes::SQLITE_ENGINE_ERROR,
            "{}. Status: {}. Message: {}",
            message,
            status,
            db ? sqlite3_errmsg(db) : sqlite3_errstr(status));
    }
}

SQLitePtr openSQLiteMemoryDatabase()
{
    sqlite3 * db = nullptr;
    int status = sqlite3_open(":memory:", &db);
    if (status != SQLITE_OK)
    {
        String message = db ? sqlite3_errmsg(db) : sqlite3_errstr(status);
        if (db)
            sqlite3_close(db);
        throw Exception(ErrorCodes::SQLITE_ENGINE_ERROR, "Cannot open in-memory SQLite database. Status: {}. Message: {}", status, message);
    }
    return SQLitePtr(db, sqlite3_close);
}

SQLitePtr deserializeSQLiteDatabase(ReadBuffer & in)
{
    String data;
    readStringUntilEOF(data, in);

    auto db = openSQLiteMemoryDatabase();

    auto * sqlite_data = static_cast<unsigned char *>(sqlite3_malloc64(data.size()));
    if (!sqlite_data && !data.empty())
        throw Exception(ErrorCodes::SQLITE_ENGINE_ERROR, "Cannot allocate {} bytes for SQLite database", data.size());

    if (!data.empty())
        memcpy(sqlite_data, data.data(), data.size());

    int status = sqlite3_deserialize(
        db.get(),
        "main",
        sqlite_data,
        static_cast<sqlite3_int64>(data.size()),
        static_cast<sqlite3_int64>(data.size()),
        SQLITE_DESERIALIZE_FREEONCLOSE);

    if (status != SQLITE_OK)
        checkSQLiteStatus(db.get(), status, "Cannot deserialize SQLite database");

    return db;
}

void executeSQLite(sqlite3 * db, const String & query)
{
    char * err_message = nullptr;
    int status = sqlite3_exec(db, query.c_str(), nullptr, nullptr, &err_message);

    if (status != SQLITE_OK)
    {
        String message(err_message ? err_message : sqlite3_errmsg(db));
        sqlite3_free(err_message);
        throw Exception(
            ErrorCodes::SQLITE_ENGINE_ERROR, "Cannot execute SQLite query: {}. Status: {}. Message: {}", query, status, message);
    }
}

SQLiteStatementPtr prepareSQLiteStatement(sqlite3 * db, const String & query)
{
    sqlite3_stmt * statement = nullptr;
    int status = sqlite3_prepare_v2(db, query.c_str(), static_cast<int>(query.size() + 1), &statement, nullptr);
    checkSQLiteStatus(db, status, fmt::format("Cannot prepare SQLite query: {}", query));
    return SQLiteStatementPtr(statement, sqlite3_finalize);
}

String sqliteTypeName(const DataTypePtr & type)
{
    auto nested_type = removeLowCardinalityAndNullable(type);
    WhichDataType which(nested_type);

    if (which.isUInt64() || which.isInt128() || which.isUInt128() || which.isInt256() || which.isUInt256())
        return "TEXT";

    if (which.isInt() || which.isUInt())
        return "INTEGER";

    if (which.isFloat())
        return "REAL";

    return "TEXT";
}

String makeCreateTableQuery(const Block & header, const String & table_name)
{
    WriteBufferFromOwnString query;
    writeCString("CREATE TABLE ", query);
    writeString(doubleQuoteString(table_name), query);
    writeCString(" (", query);

    for (size_t i = 0; i != header.columns(); ++i)
    {
        if (i)
            writeCString(", ", query);

        const auto & column = header.getByPosition(i);
        writeString(doubleQuoteString(column.name), query);
        writeChar(' ', query);
        writeString(sqliteTypeName(column.type), query);

        if (!column.type->isNullable() && !column.type->isLowCardinalityNullable())
            writeCString(" NOT NULL", query);
    }

    writeChar(')', query);
    return query.str();
}

String makeInsertQuery(const Block & header, const String & table_name)
{
    WriteBufferFromOwnString query;
    writeCString("INSERT INTO ", query);
    writeString(doubleQuoteString(table_name), query);
    writeCString(" (", query);

    for (size_t i = 0; i != header.columns(); ++i)
    {
        if (i)
            writeCString(", ", query);
        writeString(doubleQuoteString(header.getByPosition(i).name), query);
    }

    writeCString(") VALUES (", query);
    for (size_t i = 0; i != header.columns(); ++i)
    {
        if (i)
            writeCString(", ", query);
        writeChar('?', query);
    }

    writeChar(')', query);
    return query.str();
}

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
    {
        for (const auto & column : *header)
            serializations.emplace_back(column.type->getDefaultSerialization());
    }

    String getName() const override { return "SQLite"; }

    Chunk read() override
    {
        if (sqlite_finished)
            return {};

        if (!initialized)
            initialize();

        MutableColumns columns = header->cloneEmptyColumns();
        size_t num_rows = 0;

        while (num_rows < max_block_size)
        {
            int status = sqlite3_step(statement.get());

            if (status == SQLITE_DONE)
            {
                statement.reset();
                sqlite_finished = true;
                return num_rows ? Chunk(std::move(columns), num_rows) : Chunk{};
            }

            checkSQLiteStatus(sqlite_db.get(), status, "Cannot read row from SQLite database");

            for (size_t column_index = 0; column_index != columns.size(); ++column_index)
            {
                if (sqlite3_column_type(statement.get(), static_cast<int>(column_index)) == SQLITE_NULL)
                {
                    columns[column_index]->insertDefault();
                    continue;
                }

                const char * data = reinterpret_cast<const char *>(sqlite3_column_text(statement.get(), static_cast<int>(column_index)));
                int size = sqlite3_column_bytes(statement.get(), static_cast<int>(column_index));
                if (!data && size)
                    throw Exception(ErrorCodes::SQLITE_ENGINE_ERROR, "Cannot read text value from SQLite database");

                ReadBufferFromString value(std::string_view(data ? data : "", size));
                serializations[column_index]->deserializeWholeText(*columns[column_index], value, settings);
            }

            ++num_rows;
        }

        return Chunk(std::move(columns), num_rows);
    }

private:
    void initialize()
    {
        sqlite_db = deserializeSQLiteDatabase(*in);
        statement = prepareSQLiteStatement(sqlite_db.get(), makeSelectQuery(*header, settings.sqlite.input_table_name));
        initialized = true;
    }

    SharedHeader header;
    FormatSettings settings;
    UInt64 max_block_size;
    std::vector<SerializationPtr> serializations;
    SQLitePtr sqlite_db{nullptr, sqlite3_close};
    SQLiteStatementPtr statement{nullptr, sqlite3_finalize};
    bool initialized = false;
    bool sqlite_finished = false;
};

class SQLiteOutputFormat final : public IOutputFormat
{
public:
    SQLiteOutputFormat(WriteBuffer & out_, SharedHeader header_, const FormatSettings & settings_)
        : IOutputFormat(header_, out_)
        , header(std::move(header_))
        , settings(settings_)
        , sqlite_db(openSQLiteMemoryDatabase())
    {
        for (const auto & column : *header)
            serializations.emplace_back(column.type->getDefaultSerialization());
    }

    String getName() const override { return "SQLite"; }

    void writePrefix() override
    {
        executeSQLite(sqlite_db.get(), makeCreateTableQuery(*header, settings.sqlite.output_table_name));
        executeSQLite(sqlite_db.get(), "BEGIN");
        insert_statement = prepareSQLiteStatement(sqlite_db.get(), makeInsertQuery(*header, settings.sqlite.output_table_name));
    }

    void consume(Chunk chunk) override
    {
        const auto columns = chunk.getColumns();

        for (size_t row = 0; row != chunk.getNumRows(); ++row)
        {
            sqlite3_clear_bindings(insert_statement.get());
            sqlite3_reset(insert_statement.get());

            for (size_t column_index = 0; column_index != columns.size(); ++column_index)
            {
                int sqlite_index = static_cast<int>(column_index + 1);
                if (columns[column_index]->isNullAt(row))
                {
                    checkSQLiteStatus(sqlite_db.get(), sqlite3_bind_null(insert_statement.get(), sqlite_index), "Cannot bind NULL value");
                    continue;
                }

                WriteBufferFromOwnString value;
                serializations[column_index]->serializeText(*columns[column_index], row, value, settings);
                const auto value_string = value.str();
                checkSQLiteStatus(
                    sqlite_db.get(),
                    sqlite3_bind_text(
                        insert_statement.get(), sqlite_index, value_string.data(), static_cast<int>(value_string.size()), SQLITE_TRANSIENT),
                    "Cannot bind text value");
            }

            int status = sqlite3_step(insert_statement.get());
            checkSQLiteStatus(sqlite_db.get(), status, "Cannot insert row into SQLite database");
        }
    }

    void writeSuffix() override
    {
        insert_statement.reset();
        executeSQLite(sqlite_db.get(), "COMMIT");

        sqlite3_int64 size = 0;
        unsigned char * data = sqlite3_serialize(sqlite_db.get(), "main", &size, 0);
        if (!data)
            throw Exception(ErrorCodes::SQLITE_ENGINE_ERROR, "Cannot serialize SQLite database");

        SCOPE_EXIT({ sqlite3_free(data); });
        out.write(reinterpret_cast<const char *>(data), size);
    }

private:
    SharedHeader header;
    FormatSettings settings;
    std::vector<SerializationPtr> serializations;
    SQLitePtr sqlite_db;
    SQLiteStatementPtr insert_statement{nullptr, sqlite3_finalize};
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
        auto db = deserializeSQLiteDatabase(in);
        auto columns = fetchSQLiteTableStructure(db.get(), settings.sqlite.input_table_name);

        if (!columns)
            throw Exception(
                ErrorCodes::SQLITE_ENGINE_ERROR, "Cannot fetch table structure for SQLite table {}", settings.sqlite.input_table_name);

        return *columns;
    }

private:
    FormatSettings settings;
};

}

void registerInputFormatSQLite(FormatFactory & factory)
{
    factory.registerInputFormat(
        "SQLite",
        [](ReadBuffer & buf, const Block & sample, const RowInputFormatParams & params, const FormatSettings & settings)
        { return std::make_shared<SQLiteInputFormat>(buf, std::make_shared<const Block>(sample), settings, params.max_block_size_rows); });

    factory.registerFileExtension("sqlite", "SQLite");
    factory.registerFileExtension("sqlite3", "SQLite");
}

void registerOutputFormatSQLite(FormatFactory & factory)
{
    factory.registerOutputFormat(
        "SQLite",
        [](WriteBuffer & buf, const Block & sample, const FormatSettings & settings, FormatFilterInfoPtr /*format_filter_info*/)
        { return std::make_shared<SQLiteOutputFormat>(buf, std::make_shared<const Block>(sample), settings); });

    factory.markOutputFormatNotTTYFriendly("SQLite");
    factory.setContentType("SQLite", "application/vnd.sqlite3");
}

void registerSQLiteSchemaReader(FormatFactory & factory)
{
    factory.registerSchemaReader(
        "SQLite", [](ReadBuffer & buf, const FormatSettings & settings) { return std::make_shared<SQLiteSchemaReader>(buf, settings); });
}

}

#endif
