#include "config.h"

#if USE_SQLITE

#    include <Columns/IColumn.h>
#    include <DataTypes/DataTypeLowCardinality.h>
#    include <DataTypes/DataTypeNullable.h>
#    include <DataTypes/IDataType.h>
#    include <DataTypes/Serializations/ISerialization.h>
#    include <Formats/FormatFactory.h>
#    include <IO/WriteBufferFromFile.h>
#    include <IO/WriteBufferFromString.h>
#    include <IO/WriteHelpers.h>
#    include <Processors/Formats/IOutputFormat.h>
#    include <Processors/Formats/Impl/SQLiteCommon.h>
#    include <Common/quoteString.h>

#    include <sys/stat.h>

namespace DB
{

namespace ErrorCodes
{
extern const int SQLITE_ENGINE_ERROR;
}

namespace
{

using namespace SQLiteFormatImpl;

SQLitePtr openSQLiteDatabaseForOutput(WriteBuffer & out, bool & write_serialized_database_to_output)
{
    if (auto * file_out = dynamic_cast<WriteBufferFromFile *>(&out))
    {
        const auto file_name = file_out->getFileName();
        struct stat statbuf;
        if (fstat(file_out->getFD(), &statbuf) == 0 && S_ISREG(statbuf.st_mode) && !file_name.empty() && !file_name.starts_with("(fd = "))
        {
            write_serialized_database_to_output = false;
            return openSQLiteDatabase(file_name);
        }
    }

    write_serialized_database_to_output = true;
    return openSQLiteDatabase(":memory:");
}

void writeSerializedSQLiteDatabase(sqlite3 * db, WriteBuffer & out)
{
    sqlite3_int64 database_size = 0;
    unsigned char * data = sqlite3_serialize(db, "main", &database_size, 0);
    if (!data)
        throw Exception(ErrorCodes::SQLITE_ENGINE_ERROR, "Cannot serialize SQLite database to memory");

    std::unique_ptr<unsigned char, decltype(&sqlite3_free)> serialized_database(data, sqlite3_free);
    out.write(reinterpret_cast<const char *>(serialized_database.get()), static_cast<size_t>(database_size));
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

        if (!canContainNull(*column.type))
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

void bindSQLiteValue(
    sqlite3 * db,
    sqlite3_stmt * statement,
    int sqlite_index,
    const IColumn & column,
    size_t row,
    const DataTypePtr & type,
    const ISerialization & serialization,
    const FormatSettings & settings)
{
    auto nested_type = removeLowCardinalityAndNullable(type);
    WhichDataType which(nested_type);

    if (isBool(nested_type))
    {
        checkSQLiteStatus(
            db,
            sqlite3_bind_int64(statement, sqlite_index, column[row].safeGet<UInt64>() != 0),
            "Cannot bind boolean value");
        return;
    }

    if (which.isNativeInt())
    {
        checkSQLiteStatus(
            db,
            sqlite3_bind_int64(statement, sqlite_index, column[row].safeGet<Int64>()),
            "Cannot bind integer value");
        return;
    }

    if (which.isUInt8() || which.isUInt16() || which.isUInt32())
    {
        checkSQLiteStatus(
            db,
            sqlite3_bind_int64(statement, sqlite_index, static_cast<sqlite3_int64>(column[row].safeGet<UInt64>())),
            "Cannot bind unsigned integer value");
        return;
    }

    if (which.isFloat())
    {
        checkSQLiteStatus(
            db,
            sqlite3_bind_double(statement, sqlite_index, column[row].safeGet<Float64>()),
            "Cannot bind floating-point value");
        return;
    }

    WriteBufferFromOwnString value;
    serialization.serializeText(column, row, value, settings);
    const auto value_string = value.str();
    checkSQLiteStatus(
        db,
        sqlite3_bind_text(statement, sqlite_index, value_string.data(), static_cast<int>(value_string.size()), SQLITE_TRANSIENT),
        "Cannot bind text value");
}

class SQLiteOutputFormat final : public IOutputFormat
{
public:
    SQLiteOutputFormat(WriteBuffer & out_, SharedHeader header_, const FormatSettings & settings_)
        : IOutputFormat(header_, out_)
        , header(std::move(header_))
        , settings(settings_)
        , sqlite_db(openSQLiteDatabaseForOutput(out, write_serialized_database_to_output))
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
        const auto & columns = chunk.getColumns();

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

                bindSQLiteValue(
                    sqlite_db.get(),
                    insert_statement.get(),
                    sqlite_index,
                    *columns[column_index],
                    row,
                    header->getByPosition(column_index).type,
                    *serializations[column_index],
                    settings);
            }

            int status = sqlite3_step(insert_statement.get());
            checkSQLiteStatus(sqlite_db.get(), status, "Cannot insert row into SQLite database");
        }
    }

    void writeSuffix() override
    {
        insert_statement.reset();
        executeSQLite(sqlite_db.get(), "COMMIT");

        if (write_serialized_database_to_output)
            writeSerializedSQLiteDatabase(sqlite_db.get(), out);

        sqlite_db.reset();
    }

private:
    SharedHeader header;
    FormatSettings settings;
    std::vector<SerializationPtr> serializations;
    bool write_serialized_database_to_output = false;
    SQLitePtr sqlite_db;
    SQLiteStatementPtr insert_statement{nullptr, sqlite3_finalize};
};

}

void registerOutputFormatSQLite(FormatFactory & factory)
{
    factory.registerOutputFormat(
        "SQLite",
        [](WriteBuffer & buf, const Block & sample, const FormatSettings & settings, FormatFilterInfoPtr /*format_filter_info*/)
        { return std::make_shared<SQLiteOutputFormat>(buf, std::make_shared<const Block>(sample), settings); });

    factory.markFormatHasNoAppendSupport("SQLite");
    factory.markOutputFormatNotTTYFriendly("SQLite");
    factory.setContentType("SQLite", "application/vnd.sqlite3");
}

}

#endif
