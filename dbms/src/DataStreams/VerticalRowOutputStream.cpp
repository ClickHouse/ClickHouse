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
#include <cmath>

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <DataStreams/VerticalRowOutputStream.h>
#include <Common/UTF8Helpers.h>


namespace DB
{

VerticalRowOutputStream::VerticalRowOutputStream(
    WriteBuffer & ostr_, const Block & sample_, size_t max_rows_, const Context & context)
    : ostr(ostr_), sample(sample_), max_rows(max_rows_)
{
    size_t columns = sample.columns();

    using Widths = std::vector<size_t>;
    Widths name_widths(columns);
    size_t max_name_width = 0;

    String serialized_value;

    for (size_t i = 0; i < columns; ++i)
    {
        /// Note that number of code points is just a rough approximation of visible string width.
        const String & name = sample.getByPosition(i).name;

        {
            /// We need to obtain length in escaped form.
            WriteBufferFromString out(serialized_value);
            writeEscapedString(name, out);
        }

        name_widths[i] = UTF8::countCodePoints(reinterpret_cast<const UInt8 *>(serialized_value.data()), serialized_value.size());

        if (name_widths[i] > max_name_width)
            max_name_width = name_widths[i];
    }

    names_and_paddings.resize(columns);
    for (size_t i = 0; i < columns; ++i)
    {
        WriteBufferFromString out(names_and_paddings[i]);
        writeEscapedString(sample.getByPosition(i).name, out);
        writeCString(": ", out);
    }

    for (size_t i = 0; i < columns; ++i)
        names_and_paddings[i].resize(max_name_width + strlen(": "), ' ');
}


void VerticalRowOutputStream::flush()
{
    ostr.next();
}


void VerticalRowOutputStream::writeField(const String & name, const IColumn & column, const IDataType & type, size_t row_num)
{
    if (row_number > max_rows)
        return;

    writeString(name, ostr);
    writeValue(column, type, row_num);
    writeChar('\n', ostr);
}


void VerticalRowOutputStream::writeValue(const IColumn & column, const IDataType & type, size_t row_num) const
{
    type.serializeTextEscaped(column, row_num, ostr);
}

void VerticalRawRowOutputStream::writeValue(const IColumn & column, const IDataType & type, size_t row_num) const
{
    type.serializeText(column, row_num, ostr);
}


void VerticalRowOutputStream::writeRowStartDelimiter()
{
    ++row_number;

    if (row_number > max_rows)
        return;

    writeCString("Row ", ostr);
    writeIntText(row_number, ostr);
    writeCString(":\n", ostr);

    size_t width = log10(row_number + 1) + 1 + strlen("Row :");
    for (size_t i = 0; i < width; ++i)
        writeCString("─", ostr);
    writeChar('\n', ostr);
}


void VerticalRowOutputStream::writeRowBetweenDelimiter()
{
    if (row_number > max_rows)
        return;

    writeCString("\n", ostr);
}


void VerticalRowOutputStream::writeSuffix()
{
    if (row_number > max_rows)
    {
        writeCString("Showed first ", ostr);
        writeIntText(max_rows, ostr);
        writeCString(".\n", ostr);
    }

    if (totals || extremes)
    {
        writeCString("\n", ostr);
        writeTotals();
        writeExtremes();
    }

    flush();
}


void VerticalRowOutputStream::writeSpecialRow(const Block & block, size_t row_num, const char * title)
{
    writeCString("\n", ostr);

    row_number = 0;

    size_t columns = block.columns();

    writeCString(title, ostr);
    writeCString(":\n", ostr);

    size_t width = strlen(title) + 1;
    for (size_t i = 0; i < width; ++i)
        writeCString("─", ostr);
    writeChar('\n', ostr);

    for (size_t i = 0; i < columns; ++i)
    {
        if (i != 0)
            writeFieldDelimiter();

        auto & col = block.getByPosition(i);
        writeField(col.name, *col.column, *col.type, row_num);
    }
}


void VerticalRowOutputStream::writeTotals()
{
    if (totals)
    {
        writeSpecialRow(totals, 0, "Totals");
    }
}


void VerticalRowOutputStream::writeExtremes()
{
    if (extremes)
    {
        writeSpecialRow(extremes, 0, "Min");
        writeSpecialRow(extremes, 1, "Max");
    }
}


}
