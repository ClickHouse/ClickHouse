#include <Processors/Formats/Impl/TemplateBlockOutputFormat.h>
#include <Formats/FormatFactory.h>
#include <IO/WriteHelpers.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}

TemplateBlockOutputFormat::TemplateBlockOutputFormat(const Block & header_, WriteBuffer & out_, const FormatSettings & settings_)
        : IOutputFormat(header_, out_), settings(settings_)
{
    auto & sample = getPort(PortKind::Main).getHeader();
    size_t columns = sample.columns();
    types.resize(columns);
    for (size_t i = 0; i < columns; ++i)
        types[i] = sample.safeGetByPosition(i).type;

    /// Parse format string for whole output
    static const String default_format("${data}");
    const String & format_str = settings.template_settings.format.empty() ? default_format : settings.template_settings.format;
    format = ParsedTemplateFormatString(format_str, [&](const String & partName)
    {
        return static_cast<size_t>(stringToOutputPart(partName));
    });

    /// Validate format string for whole output
    size_t data_idx = format.format_idx_to_column_idx.size() + 1;
    for (size_t i = 0; i < format.format_idx_to_column_idx.size(); ++i)
    {
        if (!format.format_idx_to_column_idx[i])
            format.throwInvalidFormat("Output part name cannot be empty, it's a bug.", i);
        switch (static_cast<OutputPart>(*format.format_idx_to_column_idx[i]))
        {
            case OutputPart::Data:
                data_idx = i;
                [[fallthrough]];
            case OutputPart::Totals:
            case OutputPart::ExtremesMin:
            case OutputPart::ExtremesMax:
                if (format.formats[i] != ColumnFormat::None)
                    format.throwInvalidFormat("Serialization type for data, totals, min and max must be empty or None", i);
                break;
            default:
                if (format.formats[i] == ColumnFormat::None)
                    format.throwInvalidFormat("Serialization type for output part rows, rows_before_limit, time, "
                                              "rows_read or bytes_read is not specified", i);
                break;
        }
    }
    if (data_idx != 0)
        format.throwInvalidFormat("${data} must be the first output part", 0);

    /// Parse format string for rows
    row_format = ParsedTemplateFormatString(settings.template_settings.row_format, [&](const String & colName)
    {
        return sample.getPositionByName(colName);
    });

    /// Validate format string for rows
    if (row_format.delimiters.size() == 1)
        row_format.throwInvalidFormat("No columns specified", 0);
    for (size_t i = 0; i < row_format.columnsCount(); ++i)
    {
        if (!row_format.format_idx_to_column_idx[i])
            row_format.throwInvalidFormat("Cannot skip format field for output, it's a bug.", i);
        if (row_format.formats[i] == ColumnFormat::None)
            row_format.throwInvalidFormat("Serialization type for file column is not specified", i);
    }
}

TemplateBlockOutputFormat::OutputPart TemplateBlockOutputFormat::stringToOutputPart(const String & part)
{
    if (part == "data")
        return OutputPart::Data;
    else if (part == "totals")
        return OutputPart::Totals;
    else if (part == "min")
        return OutputPart::ExtremesMin;
    else if (part == "max")
        return OutputPart::ExtremesMax;
    else if (part == "rows")
        return OutputPart::Rows;
    else if (part == "rows_before_limit")
        return OutputPart::RowsBeforeLimit;
    else if (part == "time")
        return OutputPart::TimeElapsed;
    else if (part == "rows_read")
        return OutputPart::RowsRead;
    else if (part == "bytes_read")
        return OutputPart::BytesRead;
    else
        throw Exception("Unknown output part " + part, ErrorCodes::SYNTAX_ERROR);
}

void TemplateBlockOutputFormat::writeRow(const Chunk & chunk, size_t row_num)
{
    size_t columns = row_format.format_idx_to_column_idx.size();
    for (size_t j = 0; j < columns; ++j)
    {
        writeString(row_format.delimiters[j], out);

        size_t col_idx = *row_format.format_idx_to_column_idx[j];
        serializeField(*chunk.getColumns()[col_idx], *types[col_idx], row_num, row_format.formats[j]);
    }
    writeString(row_format.delimiters[columns], out);
}

void TemplateBlockOutputFormat::serializeField(const IColumn & column, const IDataType & type, size_t row_num, ColumnFormat col_format)
{
    switch (col_format)
    {
        case ColumnFormat::Escaped:
            type.serializeAsTextEscaped(column, row_num, out, settings);
            break;
        case ColumnFormat::Quoted:
            type.serializeAsTextQuoted(column, row_num, out, settings);
            break;
        case ColumnFormat::Csv:
            type.serializeAsTextCSV(column, row_num, out, settings);
            break;
        case ColumnFormat::Json:
            type.serializeAsTextJSON(column, row_num, out, settings);
            break;
        case ColumnFormat::Xml:
            type.serializeAsTextXML(column, row_num, out, settings);
            break;
        case ColumnFormat::Raw:
            type.serializeAsText(column, row_num, out, settings);
            break;
        default:
            __builtin_unreachable();
    }
}

template <typename U, typename V> void TemplateBlockOutputFormat::writeValue(U value, ColumnFormat col_format)
{
    auto type = std::make_unique<V>();
    auto col = type->createColumn();
    col->insert(value);
    serializeField(*col, *type, 0, col_format);
}

void TemplateBlockOutputFormat::consume(Chunk chunk)
{
    doWritePrefix();

    size_t rows = chunk.getNumRows();

    for (size_t i = 0; i < rows; ++i)
    {
        if (row_count)
            writeString(settings.template_settings.row_between_delimiter, out);

        writeRow(chunk, i);
        ++row_count;
    }
}

void TemplateBlockOutputFormat::doWritePrefix()
{
    if (need_write_prefix)
    {
        writeString(format.delimiters.front(), out);
        need_write_prefix = false;
    }
}

void TemplateBlockOutputFormat::finalize()
{
    if (finalized)
        return;

    doWritePrefix();

    size_t parts = format.format_idx_to_column_idx.size();

    for (size_t i = 0; i < parts; ++i)
    {
        auto type = std::make_shared<DataTypeUInt64>();
        ColumnWithTypeAndName col(type->createColumnConst(1, row_count), type, String("tmp"));
        switch (static_cast<OutputPart>(*format.format_idx_to_column_idx[i]))
        {
            case OutputPart::Totals:
                if (!totals)
                    format.throwInvalidFormat("Cannot print totals for this request", i);
                writeRow(totals, 0);
                break;
            case OutputPart::ExtremesMin:
                if (!extremes)
                    format.throwInvalidFormat("Cannot print extremes for this request", i);
                writeRow(extremes, 0);
                break;
            case OutputPart::ExtremesMax:
                if (!extremes)
                    format.throwInvalidFormat("Cannot print extremes for this request", i);
                writeRow(extremes, 1);
                break;
            case OutputPart::Rows:
                writeValue<size_t, DataTypeUInt64>(row_count, format.formats[i]);
                break;
            case OutputPart::RowsBeforeLimit:
                if (!rows_before_limit_set)
                    format.throwInvalidFormat("Cannot print rows_before_limit for this request", i);
                writeValue<size_t, DataTypeUInt64>(rows_before_limit, format.formats[i]);
                break;
            case OutputPart::TimeElapsed:
                writeValue<double, DataTypeFloat64>(watch.elapsedSeconds(), format.formats[i]);
                break;
            case OutputPart::RowsRead:
                writeValue<size_t, DataTypeUInt64>(progress.read_rows.load(), format.formats[i]);
                break;
            case OutputPart::BytesRead:
                writeValue<size_t, DataTypeUInt64>(progress.read_bytes.load(), format.formats[i]);
                break;
            default:
                break;
        }
        writeString(format.delimiters[i + 1], out);
    }

    finalized = true;
}


void registerOutputFormatProcessorTemplate(FormatFactory & factory)
{
    factory.registerOutputFormatProcessor("Template", [](
            WriteBuffer & buf,
            const Block & sample,
            const Context &,
            FormatFactory::WriteCallback,
            const FormatSettings & settings)
    {
        return std::make_shared<TemplateBlockOutputFormat>(sample, buf, settings);
    });
}
}
