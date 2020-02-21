#include <IO/WriteHelpers.h>
#include <IO/WriteBufferValidUTF8.h>
#include <Processors/Formats/Impl/XMLRowOutputFormat.h>
#include <Formats/FormatFactory.h>


namespace DB
{

XMLRowOutputFormat::XMLRowOutputFormat(WriteBuffer & out_, const Block & header_, FormatFactory::WriteCallback callback, const FormatSettings & format_settings_)
    : IRowOutputFormat(header_, out_, callback), format_settings(format_settings_)
{
    auto & sample = getPort(PortKind::Main).getHeader();
    NamesAndTypesList columns(sample.getNamesAndTypesList());
    fields.assign(columns.begin(), columns.end());
    field_tag_names.resize(sample.columns());

    bool need_validate_utf8 = false;
    for (size_t i = 0; i < sample.columns(); ++i)
    {
        if (!sample.getByPosition(i).type->textCanContainOnlyValidUTF8())
            need_validate_utf8 = true;

        /// As element names, we will use the column name if it has a valid form, or "field", otherwise.
        /// The condition below is more strict than the XML standard requires.
        bool is_column_name_suitable = true;
        const char * begin = fields[i].name.data();
        const char * end = begin + fields[i].name.size();
        for (const char * pos = begin; pos != end; ++pos)
        {
            char c = *pos;
            if (!(isAlphaASCII(c)
                || (pos != begin && isNumericASCII(c))
                || c == '_'
                || c == '-'
                || c == '.'))
            {
                is_column_name_suitable = false;
                break;
            }
        }

        field_tag_names[i] = is_column_name_suitable
            ? fields[i].name
            : "field";
    }

    if (need_validate_utf8)
    {
        validating_ostr = std::make_unique<WriteBufferValidUTF8>(out);
        ostr = validating_ostr.get();
    }
    else
        ostr = &out;
}


void XMLRowOutputFormat::writePrefix()
{
    writeCHCString("<?xml version='1.0' encoding='UTF-8' ?>\n", *ostr);
    writeCHCString("<result>\n", *ostr);
    writeCHCString("\t<meta>\n", *ostr);
    writeCHCString("\t\t<columns>\n", *ostr);

    for (const auto & field : fields)
    {
        writeCHCString("\t\t\t<column>\n", *ostr);

        writeCHCString("\t\t\t\t<name>", *ostr);
        writeXMLString(field.name, *ostr);
        writeCHCString("</name>\n", *ostr);
        writeCHCString("\t\t\t\t<type>", *ostr);
        writeXMLString(field.type->getName(), *ostr);
        writeCHCString("</type>\n", *ostr);

        writeCHCString("\t\t\t</column>\n", *ostr);
    }

    writeCHCString("\t\t</columns>\n", *ostr);
    writeCHCString("\t</meta>\n", *ostr);
    writeCHCString("\t<data>\n", *ostr);
}


void XMLRowOutputFormat::writeField(const IColumn & column, const IDataType & type, size_t row_num)
{
    writeCHCString("\t\t\t<", *ostr);
    writeString(field_tag_names[field_number], *ostr);
    writeCHCString(">", *ostr);
    type.serializeAsTextXML(column, row_num, *ostr, format_settings);
    writeCHCString("</", *ostr);
    writeString(field_tag_names[field_number], *ostr);
    writeCHCString(">\n", *ostr);
    ++field_number;
}


void XMLRowOutputFormat::writeRowStartDelimiter()
{
    writeCHCString("\t\t<row>\n", *ostr);
}


void XMLRowOutputFormat::writeRowEndDelimiter()
{
    writeCHCString("\t\t</row>\n", *ostr);
    field_number = 0;
    ++row_count;
}


void XMLRowOutputFormat::writeSuffix()
{
    writeCHCString("\t</data>\n", *ostr);

}


void XMLRowOutputFormat::writeBeforeTotals()
{
    writeCHCString("\t<totals>\n", *ostr);
}

void XMLRowOutputFormat::writeTotals(const Columns & columns, size_t row_num)
{
    size_t totals_columns = columns.size();
    auto & header = getPort(PortKind::Totals).getHeader();
    for (size_t i = 0; i < totals_columns; ++i)
    {
        const ColumnWithTypeAndName & column = header.safeGetByPosition(i);

        writeCHCString("\t\t<", *ostr);
        writeString(field_tag_names[i], *ostr);
        writeCHCString(">", *ostr);
        column.type->serializeAsTextXML(*columns[i], row_num, *ostr, format_settings);
        writeCHCString("</", *ostr);
        writeString(field_tag_names[i], *ostr);
        writeCHCString(">\n", *ostr);
    }
}

void XMLRowOutputFormat::writeAfterTotals()
{
    writeCHCString("\t</totals>\n", *ostr);
}


void XMLRowOutputFormat::writeBeforeExtremes()
{
    writeCHCString("\t<extremes>\n", *ostr);
}

void XMLRowOutputFormat::writeMinExtreme(const Columns & columns, size_t row_num)
{
    writeExtremesElement("min", columns, row_num);
}

void XMLRowOutputFormat::writeMaxExtreme(const Columns & columns, size_t row_num)
{
    writeExtremesElement("max", columns, row_num);
}

void XMLRowOutputFormat::writeAfterExtremes()
{
    writeCHCString("\t</extremes>\n", *ostr);
}

void XMLRowOutputFormat::writeExtremesElement(const char * title, const Columns & columns, size_t row_num)
{
    auto & header = getPort(PortKind::Extremes).getHeader();

    writeCHCString("\t\t<", *ostr);
    writeCHCString(title, *ostr);
    writeCHCString(">\n", *ostr);

    size_t extremes_columns = columns.size();
    for (size_t i = 0; i < extremes_columns; ++i)
    {
        const ColumnWithTypeAndName & column = header.safeGetByPosition(i);

        writeCHCString("\t\t\t<", *ostr);
        writeString(field_tag_names[i], *ostr);
        writeCHCString(">", *ostr);
        column.type->serializeAsTextXML(*columns[i], row_num, *ostr, format_settings);
        writeCHCString("</", *ostr);
        writeString(field_tag_names[i], *ostr);
        writeCHCString(">\n", *ostr);
    }

    writeCHCString("\t\t</", *ostr);
    writeCHCString(title, *ostr);
    writeCHCString(">\n", *ostr);
}


void XMLRowOutputFormat::onProgress(const Progress & value)
{
    progress.incrementPiecewiseAtomically(value);
}

void XMLRowOutputFormat::writeLastSuffix()
{

    writeCHCString("\t<rows>", *ostr);
    writeIntText(row_count, *ostr);
    writeCHCString("</rows>\n", *ostr);

    writeRowsBeforeLimitAtLeast();

    if (format_settings.write_statistics)
        writeStatistics();

    writeCHCString("</result>\n", *ostr);
    ostr->next();
}

void XMLRowOutputFormat::writeRowsBeforeLimitAtLeast()
{
    if (applied_limit)
    {
        writeCHCString("\t<rows_before_limit_at_least>", *ostr);
        writeIntText(rows_before_limit, *ostr);
        writeCHCString("</rows_before_limit_at_least>\n", *ostr);
    }
}

void XMLRowOutputFormat::writeStatistics()
{
    writeCHCString("\t<statistics>\n", *ostr);
    writeCHCString("\t\t<elapsed>", *ostr);
    writeText(watch.elapsedSeconds(), *ostr);
    writeCHCString("</elapsed>\n", *ostr);
    writeCHCString("\t\t<rows_read>", *ostr);
    writeText(progress.read_rows.load(), *ostr);
    writeCHCString("</rows_read>\n", *ostr);
    writeCHCString("\t\t<bytes_read>", *ostr);
    writeText(progress.read_bytes.load(), *ostr);
    writeCHCString("</bytes_read>\n", *ostr);
    writeCHCString("\t</statistics>\n", *ostr);
}


void registerOutputFormatProcessorXML(FormatFactory & factory)
{
    factory.registerOutputFormatProcessor("XML", [](
        WriteBuffer & buf,
        const Block & sample,
        FormatFactory::WriteCallback callback,
        const FormatSettings & settings)
    {
        return std::make_shared<XMLRowOutputFormat>(buf, sample, callback, settings);
    });
}

}
