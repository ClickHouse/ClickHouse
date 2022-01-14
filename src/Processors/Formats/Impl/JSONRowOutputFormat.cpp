#include <IO/WriteHelpers.h>
#include <IO/WriteBufferValidUTF8.h>
#include <Processors/Formats/Impl/JSONRowOutputFormat.h>
#include <Formats/FormatFactory.h>


namespace DB
{

JSONRowOutputFormat::JSONRowOutputFormat(
    WriteBuffer & out_,
    const Block & header,
    const RowOutputFormatParams & params_,
    const FormatSettings & settings_,
    bool yield_strings_)
    : IRowOutputFormat(header, out_, params_), settings(settings_), yield_strings(yield_strings_)
{
    const auto & sample = getPort(PortKind::Main).getHeader();
    NamesAndTypesList columns(sample.getNamesAndTypesList());
    fields.assign(columns.begin(), columns.end());

    bool need_validate_utf8 = false;
    for (size_t i = 0; i < sample.columns(); ++i)
    {
        if (!sample.getByPosition(i).type->textCanContainOnlyValidUTF8())
            need_validate_utf8 = true;

        WriteBufferFromOwnString buf;
        {
            WriteBufferValidUTF8 validating_buf(buf);
            writeJSONString(fields[i].name, validating_buf, settings);
        }
        fields[i].name = buf.str();
    }

    if (need_validate_utf8)
    {
        validating_ostr = std::make_unique<WriteBufferValidUTF8>(out);
        ostr = validating_ostr.get();
    }
    else
        ostr = &out;
}


void JSONRowOutputFormat::writePrefix()
{
    writeCString("{\n", *ostr);
    writeCString("\t\"meta\":\n", *ostr);
    writeCString("\t[\n", *ostr);

    for (size_t i = 0; i < fields.size(); ++i)
    {
        writeCString("\t\t{\n", *ostr);

        writeCString("\t\t\t\"name\": ", *ostr);
        writeString(fields[i].name, *ostr);
        writeCString(",\n", *ostr);
        writeCString("\t\t\t\"type\": ", *ostr);
        writeJSONString(fields[i].type->getName(), *ostr, settings);
        writeChar('\n', *ostr);

        writeCString("\t\t}", *ostr);
        if (i + 1 < fields.size())
            writeChar(',', *ostr);
        writeChar('\n', *ostr);
    }

    writeCString("\t],\n", *ostr);
    writeChar('\n', *ostr);
    writeCString("\t\"data\":\n", *ostr);
    writeCString("\t[\n", *ostr);
}


void JSONRowOutputFormat::writeField(const IColumn & column, const ISerialization & serialization, size_t row_num)
{
    writeCString("\t\t\t", *ostr);
    writeString(fields[field_number].name, *ostr);
    writeCString(": ", *ostr);

    if (yield_strings)
    {
        WriteBufferFromOwnString buf;

        serialization.serializeText(column, row_num, buf, settings);
        writeJSONString(buf.str(), *ostr, settings);
    }
    else
        serialization.serializeTextJSON(column, row_num, *ostr, settings);

    ++field_number;
}

void JSONRowOutputFormat::writeTotalsField(const IColumn & column, const ISerialization & serialization, size_t row_num)
{
    writeCString("\t\t", *ostr);
    writeString(fields[field_number].name, *ostr);
    writeCString(": ", *ostr);

    if (yield_strings)
    {
        WriteBufferFromOwnString buf;

        serialization.serializeText(column, row_num, buf, settings);
        writeJSONString(buf.str(), *ostr, settings);
    }
    else
        serialization.serializeTextJSON(column, row_num, *ostr, settings);

    ++field_number;
}

void JSONRowOutputFormat::writeFieldDelimiter()
{
    writeCString(",\n", *ostr);
}


void JSONRowOutputFormat::writeRowStartDelimiter()
{
    writeCString("\t\t{\n", *ostr);
}


void JSONRowOutputFormat::writeRowEndDelimiter()
{
    writeChar('\n', *ostr);
    writeCString("\t\t}", *ostr);
    field_number = 0;
    ++row_count;
}


void JSONRowOutputFormat::writeRowBetweenDelimiter()
{
    writeCString(",\n", *ostr);
}


void JSONRowOutputFormat::writeSuffix()
{
    writeChar('\n', *ostr);
    writeCString("\t]", *ostr);
}

void JSONRowOutputFormat::writeBeforeTotals()
{
    writeCString(",\n", *ostr);
    writeChar('\n', *ostr);
    writeCString("\t\"totals\":\n", *ostr);
    writeCString("\t{\n", *ostr);
}

void JSONRowOutputFormat::writeTotals(const Columns & columns, size_t row_num)
{
    size_t num_columns = columns.size();

    for (size_t i = 0; i < num_columns; ++i)
    {
        if (i != 0)
            writeTotalsFieldDelimiter();

        writeTotalsField(*columns[i], *serializations[i], row_num);
    }
}

void JSONRowOutputFormat::writeAfterTotals()
{
    writeChar('\n', *ostr);
    writeCString("\t}", *ostr);
    field_number = 0;
}

void JSONRowOutputFormat::writeBeforeExtremes()
{
    writeCString(",\n", *ostr);
    writeChar('\n', *ostr);
    writeCString("\t\"extremes\":\n", *ostr);
    writeCString("\t{\n", *ostr);
}

void JSONRowOutputFormat::writeExtremesElement(const char * title, const Columns & columns, size_t row_num)
{
    writeCString("\t\t\"", *ostr);
    writeCString(title, *ostr);
    writeCString("\":\n", *ostr);
    writeCString("\t\t{\n", *ostr);

    size_t extremes_columns = columns.size();
    for (size_t i = 0; i < extremes_columns; ++i)
    {
        if (i != 0)
            writeFieldDelimiter();

        writeField(*columns[i], *serializations[i], row_num);
    }

    writeChar('\n', *ostr);
    writeCString("\t\t}", *ostr);
    field_number = 0;
}

void JSONRowOutputFormat::writeMinExtreme(const Columns & columns, size_t row_num)
{
    writeExtremesElement("min", columns, row_num);
}

void JSONRowOutputFormat::writeMaxExtreme(const Columns & columns, size_t row_num)
{
    writeExtremesElement("max", columns, row_num);
}

void JSONRowOutputFormat::writeAfterExtremes()
{
    writeChar('\n', *ostr);
    writeCString("\t}", *ostr);
}

void JSONRowOutputFormat::writeLastSuffix()
{
    writeCString(",\n\n", *ostr);
    writeCString("\t\"rows\": ", *ostr);
    writeIntText(row_count, *ostr);

    writeRowsBeforeLimitAtLeast();

    if (settings.write_statistics)
        writeStatistics();

    writeChar('\n', *ostr);
    writeCString("}\n", *ostr);
    ostr->next();
}

void JSONRowOutputFormat::writeRowsBeforeLimitAtLeast()
{
    if (applied_limit)
    {
        writeCString(",\n\n", *ostr);
        writeCString("\t\"rows_before_limit_at_least\": ", *ostr);
        writeIntText(rows_before_limit, *ostr);
    }
}

void JSONRowOutputFormat::writeStatistics()
{
    writeCString(",\n\n", *ostr);
    writeCString("\t\"statistics\":\n", *ostr);
    writeCString("\t{\n", *ostr);

    writeCString("\t\t\"elapsed\": ", *ostr);
    writeText(watch.elapsedSeconds(), *ostr);
    writeCString(",\n", *ostr);
    writeCString("\t\t\"rows_read\": ", *ostr);
    writeText(progress.read_rows.load(), *ostr);
    writeCString(",\n", *ostr);
    writeCString("\t\t\"bytes_read\": ", *ostr);
    writeText(progress.read_bytes.load(), *ostr);
    writeChar('\n', *ostr);

    writeCString("\t}", *ostr);
}

void JSONRowOutputFormat::onProgress(const Progress & value)
{
    progress.incrementPiecewiseAtomically(value);
}


void registerOutputFormatProcessorJSON(FormatFactory & factory)
{
    factory.registerOutputFormatProcessor("JSON", [](
        WriteBuffer & buf,
        const Block & sample,
        const RowOutputFormatParams & params,
        const FormatSettings & format_settings)
    {
        return std::make_shared<JSONRowOutputFormat>(buf, sample, params, format_settings, false);
    });

    factory.registerOutputFormatProcessor("JSONStrings", [](
        WriteBuffer & buf,
        const Block & sample,
        const RowOutputFormatParams & params,
        const FormatSettings & format_settings)
    {
        return std::make_shared<JSONRowOutputFormat>(buf, sample, params, format_settings, true);
    });
}

}
