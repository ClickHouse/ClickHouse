#include <Formats/CSVRowOutputStream.h>
#include <Formats/FormatFactory.h>
#include <Formats/BlockOutputStreamFromRowOutputStream.h>

#include <IO/WriteHelpers.h>


namespace DB
{


CSVRowOutputStream::CSVRowOutputStream(WriteBuffer & ostr_, const Block & sample_, bool with_names_, const FormatSettings & format_settings)
    : ostr(ostr_), sample(sample_), with_names(with_names_), format_settings(format_settings)
{
    size_t columns = sample.columns();
    data_types.resize(columns);
    for (size_t i = 0; i < columns; ++i)
        data_types[i] = sample.safeGetByPosition(i).type;
}


void CSVRowOutputStream::flush()
{
    ostr.next();
}


void CSVRowOutputStream::writePrefix()
{
    size_t columns = sample.columns();

    if (with_names)
    {
        for (size_t i = 0; i < columns; ++i)
        {
            writeCSVString(sample.safeGetByPosition(i).name, ostr);
            writeChar(i == columns - 1 ? '\n' : format_settings.csv.delimiter, ostr);
        }
    }
}


void CSVRowOutputStream::writeField(const IColumn & column, const IDataType & type, size_t row_num)
{
    type.serializeTextCSV(column, row_num, ostr, format_settings);
}


void CSVRowOutputStream::writeFieldDelimiter()
{
    writeChar(format_settings.csv.delimiter, ostr);
}


void CSVRowOutputStream::writeRowEndDelimiter()
{
    writeChar('\n', ostr);
}


void CSVRowOutputStream::writeSuffix()
{
    writeTotals();
    writeExtremes();
}


void CSVRowOutputStream::writeTotals()
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


void CSVRowOutputStream::writeExtremes()
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


void registerOutputFormatCSV(FormatFactory & factory)
{
    for (bool with_names : {false, true})
    {
        factory.registerOutputFormat(with_names ? "CSVWithNames" : "CSV", [=](
            WriteBuffer & buf,
            const Block & sample,
            const Context &,
            const FormatSettings & format_settings)
        {
            return std::make_shared<BlockOutputStreamFromRowOutputStream>(
                std::make_shared<CSVRowOutputStream>(buf, sample, with_names, format_settings), sample);
        });
    }
}

}
