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
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferValidUTF8.h>
#include <DataStreams/JSONRowOutputStream.h>


namespace DB
{

JSONRowOutputStream::JSONRowOutputStream(WriteBuffer & ostr_, const Block & sample_, bool write_statistics_, const FormatSettingsJSON & settings_)
    : dst_ostr(ostr_), write_statistics(write_statistics_), settings(settings_)
{
    bool have_non_numeric_columns = false;

    sample = sample_;
    have_non_numeric_columns =setFields();

    if ( sample_.info.is_multiplexed )
    {
        is_multiplexed = true;
        have_non_numeric_columns = true; /// For multiplexed streams we have to assume the worst
    }

    if (have_non_numeric_columns)
    {
        validating_ostr = std::make_unique<WriteBufferValidUTF8>(dst_ostr);
        ostr = validating_ostr.get();
    }
    else
        ostr = &dst_ostr;
}

bool JSONRowOutputStream::setFields()
{
    bool have_non_numeric_columns = false;

    NamesAndTypesList columns(sample.getColumnsList());
    fields.assign(columns.begin(), columns.end());

    for (size_t i = 0; i < sample.columns(); ++i)
    {
        if (!sample.getByPosition(i).type->isNumeric())
            have_non_numeric_columns = true;

        WriteBufferFromOwnString out;
        writeJSONString(fields[i].name, out);

        fields[i].name = out.str();
    }
    return have_non_numeric_columns;
}

void JSONRowOutputStream::writePrefix()
{
    /// Reset row count to 0 in case we have multi query stream
    row_count = 0;
    writeChar('{', *ostr);
    writeCString(getNewlineChar(), *ostr);
    writeCString(getIndentChar(), *ostr);

    if (!sample.info.table.empty())
    {
        writeCString("\"table\":", *ostr);
        writeCString(getSpaceChar(), *ostr);
        writeJSONString(sample.info.table, *ostr);
        writeChar(',', *ostr);
        writeCString(getNewlineChar(), *ostr);
        writeCString(getNewlineChar(), *ostr);
        writeCString(getIndentChar(), *ostr);
    }

    if (!sample.info.hash.empty())
    {
        writeCString("\"hash\":", *ostr);
        writeCString(getSpaceChar(), *ostr);
        writeJSONString(sample.info.hash, *ostr);
        writeChar(',', *ostr);
        writeCString(getNewlineChar(), *ostr);
        writeCString(getNewlineChar(), *ostr);
        writeCString(getIndentChar(), *ostr);
    }

    writeCString("\"meta\":", *ostr);
    writeCString(getNewlineChar(), *ostr);
    writeCString(getIndentChar(), *ostr);
    writeChar('[', *ostr);
    writeCString(getNewlineChar(), *ostr);

    for (size_t i = 0; i < fields.size(); ++i)
    {
        writeCString(getIndentChar(), *ostr);
        writeCString(getIndentChar(), *ostr);
        writeChar('{', *ostr);
        writeCString(getNewlineChar(), *ostr);

        writeCString(getIndentChar(), *ostr);
        writeCString(getIndentChar(), *ostr);
        writeCString(getIndentChar(), *ostr);

        writeCString("\"name\":", *ostr);
        writeCString(getSpaceChar(), *ostr);

        writeString(fields[i].name, *ostr);
        writeChar(',', *ostr);
        writeCString(getNewlineChar(), *ostr);

        writeCString(getIndentChar(), *ostr);
        writeCString(getIndentChar(), *ostr);
        writeCString(getIndentChar(), *ostr);

        writeCString("\"type\":", *ostr);
        writeCString(getSpaceChar(), *ostr);
        writeJSONString(fields[i].type->getName(), *ostr);
        writeCString(getNewlineChar(), *ostr);

        writeCString(getIndentChar(), *ostr);
        writeCString(getIndentChar(), *ostr);

        writeChar('}', *ostr);
        if (i + 1 < fields.size())
            writeChar(',', *ostr);
        writeCString(getNewlineChar(), *ostr);
    }

    writeCString(getIndentChar(), *ostr);
    writeCString("],", *ostr);
    writeCString(getNewlineChar(), *ostr);

    writeCString(getNewlineChar(), *ostr);
    writeCString(getIndentChar(), *ostr);
    writeCString("\"data\":", *ostr);
    writeCString(getNewlineChar(), *ostr);

    writeCString(getIndentChar(), *ostr);
    writeChar('[', *ostr);
    writeCString(getNewlineChar(), *ostr);
}


void JSONRowOutputStream::writeField(const String & name, const IColumn & column, const IDataType & type, size_t row_num)
{
    writeCString(getIndentChar(), *ostr);
    writeCString(getIndentChar(), *ostr);
    writeCString(getIndentChar(), *ostr);
    writeJSONString(name, *ostr);
    writeChar(':', *ostr);
    writeCString(getSpaceChar(), *ostr);
    type.serializeTextJSON(column, row_num, *ostr, settings);
}


void JSONRowOutputStream::writeFieldDelimiter()
{
    writeChar(',', *ostr);
    writeCString(getNewlineChar(), *ostr);
}


void JSONRowOutputStream::writeRowStartDelimiter()
{
    if (row_count > 0)
    {
        writeChar(',', *ostr);
        writeCString(getNewlineChar(), *ostr);
    }
    writeCString(getIndentChar(), *ostr);
    writeCString(getIndentChar(), *ostr);

    writeChar('{', *ostr);
    writeCString(getNewlineChar(), *ostr);
}


void JSONRowOutputStream::writeRowEndDelimiter()
{
    writeCString(getNewlineChar(), *ostr);
    writeCString(getIndentChar(), *ostr);
    writeCString(getIndentChar(), *ostr);
    writeChar('}', *ostr);
    ++row_count;
}


void JSONRowOutputStream::writeSuffix()
{
    writeCString(getNewlineChar(), *ostr);
    writeCString(getIndentChar(), *ostr);
    writeChar(']', *ostr);

    writeTotals();
    writeExtremes();

    writeChar(',', *ostr);
    writeCString(getNewlineChar(), *ostr);
    writeCString(getNewlineChar(), *ostr);

    writeCString(getIndentChar(), *ostr);
    writeCString("\"rows\":", *ostr);
    writeCString(getSpaceChar(), *ostr);
    writeIntText(row_count, *ostr);

    writeRowsBeforeLimitAtLeast();

    if (write_statistics)
        writeStatistics();

    writeCString(getNewlineChar(), *ostr);
    writeChar('}', *ostr);
    writeCString(getSuffixChar(), *ostr);
    ostr->next();
}


void JSONRowOutputStream::writeRowsBeforeLimitAtLeast()
{
    if (applied_limit)
    {
        writeChar(',', *ostr);
        writeCString(getNewlineChar(), *ostr);
        writeCString(getNewlineChar(), *ostr);
        writeCString(getIndentChar(), *ostr);
        writeCString("\"rows_before_limit_at_least\":", *ostr);
        writeCString(getSpaceChar(), *ostr);
        writeIntText(rows_before_limit, *ostr);
    }
}


void JSONRowOutputStream::writeTotals()
{
    if (totals)
    {
        writeChar(',', *ostr);

        writeCString(getNewlineChar(), *ostr);
        writeCString(getNewlineChar(), *ostr);
        writeCString(getIndentChar(), *ostr);

        writeCString("\"totals\":", *ostr);

        writeCString(getNewlineChar(), *ostr);
        writeCString(getIndentChar(), *ostr);

        writeChar('{', *ostr);
        writeCString(getNewlineChar(), *ostr);

        size_t totals_columns = totals.columns();
        for (size_t i = 0; i < totals_columns; ++i)
        {
            const ColumnWithTypeAndName & column = totals.safeGetByPosition(i);

            if (i != 0)
            {
                writeChar(',', *ostr);
                writeCString(getNewlineChar(), *ostr);
            }
            writeCString(getIndentChar(), *ostr);
            writeCString(getIndentChar(), *ostr);
            writeJSONString(column.name, *ostr);
            writeChar(':', *ostr);
            writeCString(getSpaceChar(), *ostr);
            column.type->serializeTextJSON(*column.column.get(), 0, *ostr, settings);
        }

        writeCString(getNewlineChar(), *ostr);
        writeCString(getIndentChar(), *ostr);
        writeChar('}', *ostr);
    }
}


static void writeExtremesElement(const char * title, const Block & extremes, size_t row_num, WriteBuffer & ostr, const FormatSettingsJSON & settings,
    const char * indent_char, const char * newline_char, const char * space_char)
{
    writeCString(indent_char, ostr);
    writeCString(indent_char, ostr);
    writeChar('\"', ostr);
    writeCString(title, ostr);
    writeCString("\":", ostr);

    writeCString(newline_char, ostr);
    writeCString(indent_char, ostr);
    writeCString(indent_char, ostr);
    writeChar('{', ostr);
    writeCString(newline_char, ostr);

    size_t extremes_columns = extremes.columns();
    for (size_t i = 0; i < extremes_columns; ++i)
    {
        const ColumnWithTypeAndName & column = extremes.safeGetByPosition(i);

        if (i != 0)
        {
            writeChar(',', ostr);
            writeCString(newline_char, ostr);
        }
        writeCString(indent_char, ostr);
        writeCString(indent_char, ostr);
        writeCString(indent_char, ostr);
        writeJSONString(column.name, ostr);
        writeChar(':', ostr);
        writeCString(space_char, ostr);
        column.type->serializeTextJSON(*column.column.get(), row_num, ostr, settings);
    }

    writeCString(newline_char, ostr);
    writeCString(indent_char, ostr);
    writeCString(indent_char, ostr);
    writeChar('}', ostr);
}


void JSONRowOutputStream::writeExtremes()
{
    if (extremes)
    {
        writeChar(',', *ostr);
        writeCString(getNewlineChar(), *ostr);
        writeCString(getNewlineChar(), *ostr);

        writeCString(getIndentChar(), *ostr);
        writeCString("\"extremes\":", *ostr);
        writeCString(getNewlineChar(), *ostr);

        writeCString(getIndentChar(), *ostr);
        writeChar('{', *ostr);
        writeCString(getNewlineChar(), *ostr);

        writeExtremesElement("min", extremes, 0, *ostr, settings, getIndentChar(), getNewlineChar(), getSpaceChar());
        writeChar(',', *ostr);
        writeCString(getNewlineChar(), *ostr);
        writeExtremesElement("max", extremes, 1, *ostr, settings, getIndentChar(), getNewlineChar(), getSpaceChar());

        writeCString(getNewlineChar(), *ostr);
        writeCString(getIndentChar(), *ostr);
        writeChar('}', *ostr);
    }
}


void JSONRowOutputStream::onProgress(const Progress & value)
{
    progress.incrementPiecewiseAtomically(value);
}


void JSONRowOutputStream::onHeartbeat(const Heartbeat & heartbeat)
{
    writeChar('{', *ostr);
    writeCString(getNewlineChar(), *ostr);
    writeCString(getIndentChar(), *ostr);

    writeCString("\"heartbeat\":", *ostr);
    writeCString(getNewlineChar(), *ostr);

    writeCString(getIndentChar(), *ostr);
    writeChar('{', *ostr);
    writeCString(getNewlineChar(), *ostr);

    writeCString(getIndentChar(), *ostr);
    writeCString(getIndentChar(), *ostr);
    writeCString("\"timestamp\":", *ostr);
    writeCString(getSpaceChar(), *ostr);
    writeChar('\"', *ostr);

    writeText(heartbeat.timestamp.load(), *ostr);
    writeCString("\",", *ostr);
    writeCString(getNewlineChar(), *ostr);
    writeCString(getNewlineChar(), *ostr);
    writeCString(getIndentChar(), *ostr);
    writeCString(getIndentChar(), *ostr);

    writeCString("\"hash\":", *ostr);
    writeCString(getNewlineChar(), *ostr);
    writeCString(getIndentChar(), *ostr);
    writeCString(getIndentChar(), *ostr);
    writeChar('{', *ostr);

    for (auto it = heartbeat.hashmap.begin(); it != heartbeat.hashmap.end();)
    {
        writeCString(getNewlineChar(), *ostr);
        writeCString(getIndentChar(), *ostr);
        writeCString(getIndentChar(), *ostr);
        writeCString(getIndentChar(), *ostr);
        writeJSONString(it->first, *ostr);
        writeChar(':', *ostr);
        writeCString(getSpaceChar(), *ostr);
        writeJSONString(it->second, *ostr);
        ++it;
        if (it != heartbeat.hashmap.end())
            writeChar(',', *ostr);
    }
    writeCString(getNewlineChar(), *ostr);
    writeCString(getIndentChar(), *ostr);
    writeCString(getIndentChar(), *ostr);
    writeChar('}', *ostr);
    writeCString(getNewlineChar(), *ostr);
    writeCString(getIndentChar(), *ostr);
    writeChar('}', *ostr);
    writeCString(getNewlineChar(), *ostr);
    writeChar('}', *ostr);

    writeCString(getSuffixChar(), *ostr);
    ostr->next();
}


void JSONRowOutputStream::writeStatistics()
{
    writeChar(',', *ostr);
    writeCString(getNewlineChar(), *ostr);
    writeCString(getNewlineChar(), *ostr);

    writeCString(getIndentChar(), *ostr);
    writeCString("\"statistics\":", *ostr);
    writeCString(getNewlineChar(), *ostr);

    writeCString(getIndentChar(), *ostr);
    writeChar('{', *ostr);
    writeCString(getNewlineChar(), *ostr);

    writeCString(getIndentChar(), *ostr);
    writeCString(getIndentChar(), *ostr);
    writeCString("\"elapsed\":", *ostr);
    writeCString(getSpaceChar(), *ostr);

    writeText(watch.elapsedSeconds(), *ostr);
    writeChar(',', *ostr);
    writeCString(getNewlineChar(), *ostr);

    writeCString(getIndentChar(), *ostr);
    writeCString(getIndentChar(), *ostr);
    writeCString("\"rows_read\":", *ostr);
    writeCString(getSpaceChar(), *ostr);

    writeText(progress.rows.load(), *ostr);
    writeChar(',', *ostr);
    writeCString(getNewlineChar(), *ostr);

    writeCString(getIndentChar(), *ostr);
    writeCString(getIndentChar(), *ostr);

    writeCString("\"bytes_read\":", *ostr);
    writeCString(getSpaceChar(), *ostr);

    writeText(progress.bytes.load(), *ostr);
    writeCString(getNewlineChar(), *ostr);

    writeCString(getIndentChar(), *ostr);
    writeChar('}', *ostr);
}

}
