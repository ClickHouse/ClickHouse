/* Some modifications Copyright (c) 2018 BlackBerry Limited

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. */
#include <DataStreams/CSVRowOutputStream.h>

#include <IO/WriteHelpers.h>


namespace DB
{


CSVRowOutputStream::CSVRowOutputStream(WriteBuffer & ostr_, const Block & sample_, bool with_names_, bool with_types_)
    : ostr(ostr_), sample(sample_), with_names(with_names_), with_types(with_types_)
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
            writeChar(i == columns - 1 ? '\n' : ',', ostr);
        }
    }

    if (with_types)
    {
        for (size_t i = 0; i < columns; ++i)
        {
            writeCSVString(sample.safeGetByPosition(i).type->getName(), ostr);
            writeChar(i == columns - 1 ? '\n' : ',', ostr);
        }
    }
}


void CSVRowOutputStream::writeField(const String & name, const IColumn & column, const IDataType & type, size_t row_num)
{
    type.serializeTextCSV(column, row_num, ostr);
}


void CSVRowOutputStream::writeFieldDelimiter()
{
    writeChar(',', ostr);
}


void CSVRowOutputStream::writeRowEndDelimiter()
{
    writeChar('\n', ostr);
}


void CSVRowOutputStream::writeSuffix()
{
    writeTotals();
    writeExtremes();
    flush();
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
            writeField(totals.getByPosition(j).name, *totals.getByPosition(j).column.get(), *totals.getByPosition(j).type.get(), 0);
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
                writeField(extremes.getByPosition(j).name, *extremes.getByPosition(j).column.get(), *extremes.getByPosition(j).type.get(), i);
            }

            writeRowEndDelimiter();
        }
    }
}


}
