#include <DataStreams/CSVRowOutputStream.h>

#include <IO/WriteHelpers.h>


namespace DB
{


CSVRowOutputStream::CSVRowOutputStream(WriteBuffer & ostr_, const Block & sample_, const char delimiter_, bool with_names_, bool with_types_)
    : ostr(ostr_), sample(sample_), delimiter(delimiter_), with_names(with_names_), with_types(with_types_)
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
            writeChar(i == columns - 1 ? '\n' : delimiter, ostr);
        }
    }

    if (with_types)
    {
        for (size_t i = 0; i < columns; ++i)
        {
            writeCSVString(sample.safeGetByPosition(i).type->getName(), ostr);
            writeChar(i == columns - 1 ? '\n' : delimiter, ostr);
        }
    }
}


void CSVRowOutputStream::writeField(const IColumn & column, const IDataType & type, size_t row_num)
{
    type.serializeTextCSV(column, row_num, ostr);
}


void CSVRowOutputStream::writeFieldDelimiter()
{
    writeChar(delimiter, ostr);
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


}
