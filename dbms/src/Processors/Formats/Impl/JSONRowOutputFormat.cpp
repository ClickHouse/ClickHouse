#include <IO/WriteHelpers.h>
#include <IO/WriteBufferValidUTF8.h>
#include <Processors/Formats/Impl/JSONRowOutputFormat.h>
#include <Formats/FormatFactory.h>


namespace DB
{

JSONRowOutputFormat::JSONRowOutputFormat(WriteBuffer & out_, const Block & header, FormatFactory::WriteCallback callback, const FormatSettings & settings_)
    : IRowOutputFormat(header, out_, callback), settings(settings_)
{
    auto & sample = getPort(PortKind::Main).getHeader();
    NamesAndTypesList columns(sample.getNamesAndTypesList());
    fields.assign(columns.begin(), columns.end());

    bool need_validate_utf8 = false;
    for (size_t i = 0; i < sample.columns(); ++i)
    {
        if (!sample.getByPosition(i).type->textCanContainOnlyValidUTF8())
            need_validate_utf8 = true;

        WriteBufferFromOwnString buf;
        writeJSONString(fields[i].name, buf, settings);

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
    writeCHCString("{\n", *ostr);
    writeCHCString("\t\"meta\":\n", *ostr);
    writeCHCString("\t[\n", *ostr);

    for (size_t i = 0; i < fields.size(); ++i)
    {
        writeCHCString("\t\t{\n", *ostr);

        writeCHCString("\t\t\t\"name\": ", *ostr);
        writeString(fields[i].name, *ostr);
        writeCHCString(",\n", *ostr);
        writeCHCString("\t\t\t\"type\": ", *ostr);
        writeJSONString(fields[i].type->getName(), *ostr, settings);
        writeChar('\n', *ostr);

        writeCHCString("\t\t}", *ostr);
        if (i + 1 < fields.size())
            writeChar(',', *ostr);
        writeChar('\n', *ostr);
    }

    writeCHCString("\t],\n", *ostr);
    writeChar('\n', *ostr);
    writeCHCString("\t\"data\":\n", *ostr);
    writeCHCString("\t[\n", *ostr);
}


void JSONRowOutputFormat::writeField(const IColumn & column, const IDataType & type, size_t row_num)
{
    writeCHCString("\t\t\t", *ostr);
    writeString(fields[field_number].name, *ostr);
    writeCHCString(": ", *ostr);
    type.serializeAsTextJSON(column, row_num, *ostr, settings);
    ++field_number;
}

void JSONRowOutputFormat::writeTotalsField(const IColumn & column, const IDataType & type, size_t row_num)
{
    writeCHCString("\t\t", *ostr);
    writeString(fields[field_number].name, *ostr);
    writeCHCString(": ", *ostr);
    type.serializeAsTextJSON(column, row_num, *ostr, settings);
    ++field_number;
}

void JSONRowOutputFormat::writeFieldDelimiter()
{
    writeCHCString(",\n", *ostr);
}


void JSONRowOutputFormat::writeRowStartDelimiter()
{
    writeCHCString("\t\t{\n", *ostr);
}


void JSONRowOutputFormat::writeRowEndDelimiter()
{
    writeChar('\n', *ostr);
    writeCHCString("\t\t}", *ostr);
    field_number = 0;
    ++row_count;
}


void JSONRowOutputFormat::writeRowBetweenDelimiter()
{
    writeCHCString(",\n", *ostr);
}


void JSONRowOutputFormat::writeSuffix()
{
    writeChar('\n', *ostr);
    writeCHCString("\t]", *ostr);
}

void JSONRowOutputFormat::writeBeforeTotals()
{
    writeCHCString(",\n", *ostr);
    writeChar('\n', *ostr);
    writeCHCString("\t\"totals\":\n", *ostr);
    writeCHCString("\t{\n", *ostr);
}

void JSONRowOutputFormat::writeTotals(const Columns & columns, size_t row_num)
{
    size_t num_columns = columns.size();

    for (size_t i = 0; i < num_columns; ++i)
    {
        if (i != 0)
            writeTotalsFieldDelimiter();

        writeTotalsField(*columns[i], *types[i], row_num);
    }
}

void JSONRowOutputFormat::writeAfterTotals()
{
    writeChar('\n', *ostr);
    writeCHCString("\t}", *ostr);
    field_number = 0;
}

void JSONRowOutputFormat::writeBeforeExtremes()
{
    writeCHCString(",\n", *ostr);
    writeChar('\n', *ostr);
    writeCHCString("\t\"extremes\":\n", *ostr);
    writeCHCString("\t{\n", *ostr);
}

void JSONRowOutputFormat::writeExtremesElement(const char * title, const Columns & columns, size_t row_num)
{
    writeCHCString("\t\t\"", *ostr);
    writeCHCString(title, *ostr);
    writeCHCString("\":\n", *ostr);
    writeCHCString("\t\t{\n", *ostr);

    size_t extremes_columns = columns.size();
    for (size_t i = 0; i < extremes_columns; ++i)
    {
        if (i != 0)
            writeFieldDelimiter();

        writeField(*columns[i], *types[i], row_num);
    }

    writeChar('\n', *ostr);
    writeCHCString("\t\t}", *ostr);
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
    writeCHCString("\t}", *ostr);
}

void JSONRowOutputFormat::writeLastSuffix()
{
    writeCHCString(",\n\n", *ostr);
    writeCHCString("\t\"rows\": ", *ostr);
    writeIntText(row_count, *ostr);

    writeRowsBeforeLimitAtLeast();

    if (settings.write_statistics)
        writeStatistics();

    writeChar('\n', *ostr);
    writeCHCString("}\n", *ostr);
    ostr->next();
}

void JSONRowOutputFormat::writeRowsBeforeLimitAtLeast()
{
    if (applied_limit)
    {
        writeCHCString(",\n\n", *ostr);
        writeCHCString("\t\"rows_before_limit_at_least\": ", *ostr);
        writeIntText(rows_before_limit, *ostr);
    }
}

void JSONRowOutputFormat::writeStatistics()
{
    writeCHCString(",\n\n", *ostr);
    writeCHCString("\t\"statistics\":\n", *ostr);
    writeCHCString("\t{\n", *ostr);

    writeCHCString("\t\t\"elapsed\": ", *ostr);
    writeText(watch.elapsedSeconds(), *ostr);
    writeCHCString(",\n", *ostr);
    writeCHCString("\t\t\"rows_read\": ", *ostr);
    writeText(progress.read_rows.load(), *ostr);
    writeCHCString(",\n", *ostr);
    writeCHCString("\t\t\"bytes_read\": ", *ostr);
    writeText(progress.read_bytes.load(), *ostr);
    writeChar('\n', *ostr);

    writeCHCString("\t}", *ostr);
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
        FormatFactory::WriteCallback callback,
        const FormatSettings & format_settings)
    {
        return std::make_shared<JSONRowOutputFormat>(buf, sample, callback, format_settings);
    });
}

}
