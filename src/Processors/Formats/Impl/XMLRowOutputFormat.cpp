#include <IO/WriteHelpers.h>
#include <IO/WriteBufferValidUTF8.h>
#include <Processors/Formats/Impl/XMLRowOutputFormat.h>
#include <Formats/FormatFactory.h>


namespace DB
{

XMLRowOutputFormat::XMLRowOutputFormat(WriteBuffer & out_, const Block & header_, const RowOutputFormatParams & params_, const FormatSettings & format_settings_)
    : IRowOutputFormat(header_, out_, params_), fields(header_.getNamesAndTypes()), format_settings(format_settings_)
{
    const auto & sample = getPort(PortKind::Main).getHeader();
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
    writeCString("<?xml version='1.0' encoding='UTF-8' ?>\n", *ostr);
    writeCString("<result>\n", *ostr);
    writeCString("\t<meta>\n", *ostr);
    writeCString("\t\t<columns>\n", *ostr);

    for (const auto & field : fields)
    {
        writeCString("\t\t\t<column>\n", *ostr);

        writeCString("\t\t\t\t<name>", *ostr);
        writeXMLStringForTextElement(field.name, *ostr);
        writeCString("</name>\n", *ostr);
        writeCString("\t\t\t\t<type>", *ostr);
        writeXMLStringForTextElement(field.type->getName(), *ostr);
        writeCString("</type>\n", *ostr);

        writeCString("\t\t\t</column>\n", *ostr);
    }

    writeCString("\t\t</columns>\n", *ostr);
    writeCString("\t</meta>\n", *ostr);
    writeCString("\t<data>\n", *ostr);
}


void XMLRowOutputFormat::writeField(const IColumn & column, const ISerialization & serialization, size_t row_num)
{
    writeCString("\t\t\t<", *ostr);
    writeString(field_tag_names[field_number], *ostr);
    writeCString(">", *ostr);
    serialization.serializeTextXML(column, row_num, *ostr, format_settings);
    writeCString("</", *ostr);
    writeString(field_tag_names[field_number], *ostr);
    writeCString(">\n", *ostr);
    ++field_number;
}


void XMLRowOutputFormat::writeRowStartDelimiter()
{
    writeCString("\t\t<row>\n", *ostr);
}


void XMLRowOutputFormat::writeRowEndDelimiter()
{
    writeCString("\t\t</row>\n", *ostr);
    field_number = 0;
    ++row_count;
}


void XMLRowOutputFormat::writeSuffix()
{
    writeCString("\t</data>\n", *ostr);

}


void XMLRowOutputFormat::writeBeforeTotals()
{
    writeCString("\t<totals>\n", *ostr);
}

void XMLRowOutputFormat::writeTotals(const Columns & columns, size_t row_num)
{
    size_t totals_columns = columns.size();
    const auto & header = getPort(PortKind::Totals).getHeader();
    for (size_t i = 0; i < totals_columns; ++i)
    {
        const ColumnWithTypeAndName & column = header.safeGetByPosition(i);

        writeCString("\t\t<", *ostr);
        writeString(field_tag_names[i], *ostr);
        writeCString(">", *ostr);
        column.type->getDefaultSerialization()->serializeTextXML(*columns[i], row_num, *ostr, format_settings);
        writeCString("</", *ostr);
        writeString(field_tag_names[i], *ostr);
        writeCString(">\n", *ostr);
    }
}

void XMLRowOutputFormat::writeAfterTotals()
{
    writeCString("\t</totals>\n", *ostr);
}


void XMLRowOutputFormat::writeBeforeExtremes()
{
    writeCString("\t<extremes>\n", *ostr);
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
    writeCString("\t</extremes>\n", *ostr);
}

void XMLRowOutputFormat::writeExtremesElement(const char * title, const Columns & columns, size_t row_num)
{
    const auto & header = getPort(PortKind::Extremes).getHeader();

    writeCString("\t\t<", *ostr);
    writeCString(title, *ostr);
    writeCString(">\n", *ostr);

    size_t extremes_columns = columns.size();
    for (size_t i = 0; i < extremes_columns; ++i)
    {
        const ColumnWithTypeAndName & column = header.safeGetByPosition(i);

        writeCString("\t\t\t<", *ostr);
        writeString(field_tag_names[i], *ostr);
        writeCString(">", *ostr);
        column.type->getDefaultSerialization()->serializeTextXML(*columns[i], row_num, *ostr, format_settings);
        writeCString("</", *ostr);
        writeString(field_tag_names[i], *ostr);
        writeCString(">\n", *ostr);
    }

    writeCString("\t\t</", *ostr);
    writeCString(title, *ostr);
    writeCString(">\n", *ostr);
}


void XMLRowOutputFormat::onProgress(const Progress & value)
{
    statistics.progress.incrementPiecewiseAtomically(value);
}

void XMLRowOutputFormat::finalizeImpl()
{
    writeCString("\t<rows>", *ostr);
    writeIntText(row_count, *ostr);
    writeCString("</rows>\n", *ostr);

    auto outside_statistics = getOutsideStatistics();
    if (outside_statistics)
        statistics = std::move(*outside_statistics);

    writeRowsBeforeLimitAtLeast();

    if (format_settings.write_statistics)
        writeStatistics();

    writeCString("</result>\n", *ostr);
    ostr->next();
}

void XMLRowOutputFormat::writeRowsBeforeLimitAtLeast()
{
    if (statistics.applied_limit)
    {
        writeCString("\t<rows_before_limit_at_least>", *ostr);
        writeIntText(statistics.rows_before_limit, *ostr);
        writeCString("</rows_before_limit_at_least>\n", *ostr);
    }
}

void XMLRowOutputFormat::writeStatistics()
{
    writeCString("\t<statistics>\n", *ostr);
    writeCString("\t\t<elapsed>", *ostr);
    writeText(statistics.watch.elapsedSeconds(), *ostr);
    writeCString("</elapsed>\n", *ostr);
    writeCString("\t\t<rows_read>", *ostr);
    writeText(statistics.progress.read_rows.load(), *ostr);
    writeCString("</rows_read>\n", *ostr);
    writeCString("\t\t<bytes_read>", *ostr);
    writeText(statistics.progress.read_bytes.load(), *ostr);
    writeCString("</bytes_read>\n", *ostr);
    writeCString("\t</statistics>\n", *ostr);
}


void registerOutputFormatXML(FormatFactory & factory)
{
    factory.registerOutputFormat("XML", [](
        WriteBuffer & buf,
        const Block & sample,
        const RowOutputFormatParams & params,
        const FormatSettings & settings)
    {
        return std::make_shared<XMLRowOutputFormat>(buf, sample, params, settings);
    });

    factory.markOutputFormatSupportsParallelFormatting("XML");
    factory.markFormatHasNoAppendSupport("XML");
}

}
