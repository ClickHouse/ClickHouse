#include "config.h"

#if USE_SQLITE

#    include <Columns/IColumn.h>
#    include <DataTypes/DataTypeLowCardinality.h>
#    include <DataTypes/DataTypeNullable.h>
#    include <DataTypes/IDataType.h>
#    include <DataTypes/Serializations/ISerialization.h>
#    include <Formats/FormatFactory.h>
#    include <IO/ReadBufferFromFile.h>
#    include <IO/WriteBufferFromString.h>
#    include <IO/WriteHelpers.h>
#    include <Processors/Formats/IOutputFormat.h>
#    include <Processors/Formats/Impl/SQLiteCommon.h>
#    include <Common/quoteString.h>

namespace DB
{

namespace
{

using namespace SQLiteFormatImpl;

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

class SQLiteOutputFormat final : public IOutputFormat
{
public:
    SQLiteOutputFormat(WriteBuffer & out_, SharedHeader header_, const FormatSettings & settings_)
        : IOutputFormat(header_, out_)
        , header(std::move(header_))
        , settings(settings_)
        , sqlite_db(openSQLiteDatabase(temporary_file.getPath()))
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
        sqlite_db.reset();

        ReadBufferFromFile file_in(temporary_file.getPath());
        copyData(file_in, out);
    }

private:
    SharedHeader header;
    FormatSettings settings;
    std::vector<SerializationPtr> serializations;
    SQLiteTemporaryFile temporary_file;
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
