#include <Formats/TabSeparatedRowOutputStream.h>
#include <Formats/TabSeparatedRawRowOutputStream.h>
#include <Formats/FormatFactory.h>
#include <Formats/BlockOutputStreamFromRowOutputStream.h>

#include <IO/WriteHelpers.h>


namespace DB
{

TabSeparatedRowOutputStream::TabSeparatedRowOutputStream(
    WriteBuffer & ostr_, const Block & sample_, bool with_names_, bool with_types_, const FormatSettings & format_settings)
    : ostr(ostr_), sample(sample_), with_names(with_names_), with_types(with_types_), format_settings(format_settings)
{
}


void TabSeparatedRowOutputStream::flush()
{
    ostr.next();
}


void TabSeparatedRowOutputStream::writePrefix()
{
    size_t columns = sample.columns();

    if (with_names)
    {
        for (size_t i = 0; i < columns; ++i)
        {
            writeEscapedString(sample.safeGetByPosition(i).name, ostr);
            writeChar(i == columns - 1 ? '\n' : '\t', ostr);
        }
    }

    if (with_types)
    {
        for (size_t i = 0; i < columns; ++i)
        {
            writeEscapedString(sample.safeGetByPosition(i).type->getName(), ostr);
            writeChar(i == columns - 1 ? '\n' : '\t', ostr);
        }
    }
}


void TabSeparatedRowOutputStream::writeField(const IColumn & column, const IDataType & type, size_t row_num)
{
    type.serializeTextEscaped(column, row_num, ostr, format_settings);
}


void TabSeparatedRowOutputStream::writeFieldDelimiter()
{
    writeChar('\t', ostr);
}


void TabSeparatedRowOutputStream::writeRowEndDelimiter()
{
    writeChar('\n', ostr);
}


void TabSeparatedRowOutputStream::writeSuffix()
{
    writeTotals();
    writeExtremes();
}


void TabSeparatedRowOutputStream::writeTotals()
{
    if (totals)
    {
        size_t columns = totals.columns();

        writeChar('\n', ostr);
        writeRowStartDelimiter();

        for (size_t j = 0; j < columns; ++j)
        {
            if (j != 0)
                writeFieldDelimiter();
            writeField(*totals.getByPosition(j).column.get(), *totals.getByPosition(j).type.get(), 0);
        }

        writeRowEndDelimiter();
    }
}


void TabSeparatedRowOutputStream::writeExtremes()
{
    if (extremes)
    {
        size_t rows = extremes.rows();
        size_t columns = extremes.columns();

        writeChar('\n', ostr);

        for (size_t i = 0; i < rows; ++i)
        {
            if (i != 0)
                writeRowBetweenDelimiter();

            writeRowStartDelimiter();

            for (size_t j = 0; j < columns; ++j)
            {
                if (j != 0)
                    writeFieldDelimiter();
                writeField(*extremes.getByPosition(j).column.get(), *extremes.getByPosition(j).type.get(), i);
            }

            writeRowEndDelimiter();
        }
    }
}


void registerOutputFormatTabSeparated(FormatFactory & factory)
{
    for (auto name : {"TabSeparated", "TSV"})
    {
        factory.registerOutputFormat(name, [](
            WriteBuffer & buf,
            const Block & sample,
            const Context &,
            const FormatSettings & settings)
        {
            return std::make_shared<BlockOutputStreamFromRowOutputStream>(
                std::make_shared<TabSeparatedRowOutputStream>(buf, sample, false, false, settings), sample);
        });
    }

    for (auto name : {"TabSeparatedRaw", "TSVRaw"})
    {
        factory.registerOutputFormat(name, [](
            WriteBuffer & buf,
            const Block & sample,
            const Context &,
            const FormatSettings & settings)
        {
            return std::make_shared<BlockOutputStreamFromRowOutputStream>(
                std::make_shared<TabSeparatedRawRowOutputStream>(buf, sample, false, false, settings), sample);
        });
    }

    for (auto name : {"TabSeparatedWithNames", "TSVWithNames"})
    {
        factory.registerOutputFormat(name, [](
            WriteBuffer & buf,
            const Block & sample,
            const Context &,
            const FormatSettings & settings)
        {
            return std::make_shared<BlockOutputStreamFromRowOutputStream>(
                std::make_shared<TabSeparatedRowOutputStream>(buf, sample, true, false, settings), sample);
        });
    }

    for (auto name : {"TabSeparatedWithNamesAndTypes", "TSVWithNamesAndTypes"})
    {
        factory.registerOutputFormat(name, [](
            WriteBuffer & buf,
            const Block & sample,
            const Context &,
            const FormatSettings & settings)
        {
            return std::make_shared<BlockOutputStreamFromRowOutputStream>(
                std::make_shared<TabSeparatedRowOutputStream>(buf, sample, true, true, settings), sample);
        });
    }
}

}
