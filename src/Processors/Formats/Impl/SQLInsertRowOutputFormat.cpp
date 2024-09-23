#include <Formats/FormatFactory.h>
#include <IO/WriteHelpers.h>
#include <Processors/Formats/Impl/SQLInsertRowOutputFormat.h>


namespace DB
{

SQLInsertRowOutputFormat::SQLInsertRowOutputFormat(WriteBuffer & out_, const Block & header_, const FormatSettings & format_settings_)
    : IRowOutputFormat(header_, out_), column_names(header_.getNames()), format_settings(format_settings_)
{
}

void SQLInsertRowOutputFormat::writeRowStartDelimiter()
{
    if (rows_in_line == 0)
        printLineStart();
    writeChar('(', out);
}

void SQLInsertRowOutputFormat::printLineStart()
{
    if (format_settings.sql_insert.use_replace)
        writeCString("REPLACE INTO ", out);
    else
        writeCString("INSERT INTO ", out);

    writeString(format_settings.sql_insert.table_name, out);

    if (format_settings.sql_insert.include_column_names)
        printColumnNames();

    writeCString(" VALUES ", out);
}

void SQLInsertRowOutputFormat::printColumnNames()
{
    writeCString(" (", out);
    for (size_t i = 0; i != column_names.size(); ++i)
    {
        if (format_settings.sql_insert.quote_names)
            writeChar('`', out);

        writeString(column_names[i], out);

        if (format_settings.sql_insert.quote_names)
            writeChar('`', out);

        if (i + 1 != column_names.size())
            writeCString(", ", out);
    }
    writeChar(')', out);
}

void SQLInsertRowOutputFormat::writeField(const IColumn & column, const ISerialization & serialization, size_t row_num)
{
    serialization.serializeTextQuoted(column, row_num, out, format_settings);
}

void SQLInsertRowOutputFormat::writeFieldDelimiter()
{
    writeCString(", ", out);
}

void SQLInsertRowOutputFormat::writeRowEndDelimiter()
{
    writeChar(')', out);
    ++rows_in_line;
}

void SQLInsertRowOutputFormat::writeRowBetweenDelimiter()
{
    if (rows_in_line >= format_settings.sql_insert.max_batch_size)
    {
        writeCString(";\n", out);
        rows_in_line = 0;
    }
    else
    {
        writeCString(", ", out);
    }
}

void SQLInsertRowOutputFormat::writeSuffix()
{
    writeCString(";\n", out);
}

void SQLInsertRowOutputFormat::resetFormatterImpl()
{
    rows_in_line = 0;
}

void registerOutputFormatSQLInsert(FormatFactory & factory)
{
    factory.registerOutputFormat("SQLInsert", [](
        WriteBuffer & buf,
        const Block & sample,
        const FormatSettings & settings)
    {
        return std::make_shared<SQLInsertRowOutputFormat>(buf, sample, settings);
    });
}


}
