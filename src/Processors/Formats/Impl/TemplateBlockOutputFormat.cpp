#include <Processors/Formats/Impl/TemplateBlockOutputFormat.h>
#include <Formats/FormatFactory.h>
#include <IO/WriteHelpers.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}

TemplateBlockOutputFormat::TemplateBlockOutputFormat(const Block & header_, WriteBuffer & out_, const FormatSettings & settings_,
                                                     ParsedTemplateFormatString format_, ParsedTemplateFormatString row_format_,
                                                     std::string row_between_delimiter_)
    : IOutputFormat(header_, out_), settings(settings_), format(std::move(format_))
    , row_format(std::move(row_format_)), row_between_delimiter(std::move(row_between_delimiter_))
{
    const auto & sample = getPort(PortKind::Main).getHeader();
    size_t columns = sample.columns();
    serializations.resize(columns);
    for (size_t i = 0; i < columns; ++i)
        serializations[i] = sample.safeGetByPosition(i).type->getDefaultSerialization();

    /// Validate format string for whole output
    size_t data_idx = format.format_idx_to_column_idx.size() + 1;
    for (size_t i = 0; i < format.format_idx_to_column_idx.size(); ++i)
    {
        if (!format.format_idx_to_column_idx[i])
            format.throwInvalidFormat("Output part name cannot be empty.", i);
        switch (*format.format_idx_to_column_idx[i])
        {
            case static_cast<size_t>(ResultsetPart::Data):
                data_idx = i;
                [[fallthrough]];
            case static_cast<size_t>(ResultsetPart::Totals):
            case static_cast<size_t>(ResultsetPart::ExtremesMin):
            case static_cast<size_t>(ResultsetPart::ExtremesMax):
                if (format.formats[i] != ColumnFormat::None)
                    format.throwInvalidFormat("Serialization type for data, totals, min and max must be empty or None", i);
                break;
            case static_cast<size_t>(ResultsetPart::Rows):
            case static_cast<size_t>(ResultsetPart::RowsBeforeLimit):
            case static_cast<size_t>(ResultsetPart::TimeElapsed):
            case static_cast<size_t>(ResultsetPart::RowsRead):
            case static_cast<size_t>(ResultsetPart::BytesRead):
                if (format.formats[i] == ColumnFormat::None)
                    format.throwInvalidFormat("Serialization type for output part rows, rows_before_limit, time, "
                                              "rows_read or bytes_read is not specified", i);
                break;
            default:
                format.throwInvalidFormat("Invalid output part", i);
        }
    }
    if (data_idx != 0)
        format.throwInvalidFormat("${data} must be the first output part", 0);

    /// Validate format string for rows
    if (row_format.delimiters.size() == 1)
        row_format.throwInvalidFormat("No columns specified", 0);
    for (size_t i = 0; i < row_format.columnsCount(); ++i)
    {
        if (!row_format.format_idx_to_column_idx[i])
            row_format.throwInvalidFormat("Cannot skip format field for output, it's a bug.", i);
        if (header_.columns() <= *row_format.format_idx_to_column_idx[i])
            row_format.throwInvalidFormat("Column index " + std::to_string(*row_format.format_idx_to_column_idx[i]) +
                                          " must be less then number of columns (" + std::to_string(header_.columns()) + ")", i);
        if (row_format.formats[i] == ColumnFormat::None)
            row_format.throwInvalidFormat("Serialization type for file column is not specified", i);
    }
}

TemplateBlockOutputFormat::ResultsetPart TemplateBlockOutputFormat::stringToResultsetPart(const String & part)
{
    if (part == "data")
        return ResultsetPart::Data;
    else if (part == "totals")
        return ResultsetPart::Totals;
    else if (part == "min")
        return ResultsetPart::ExtremesMin;
    else if (part == "max")
        return ResultsetPart::ExtremesMax;
    else if (part == "rows")
        return ResultsetPart::Rows;
    else if (part == "rows_before_limit")
        return ResultsetPart::RowsBeforeLimit;
    else if (part == "time")
        return ResultsetPart::TimeElapsed;
    else if (part == "rows_read")
        return ResultsetPart::RowsRead;
    else if (part == "bytes_read")
        return ResultsetPart::BytesRead;
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
        serializeField(*chunk.getColumns()[col_idx], *serializations[col_idx], row_num, row_format.formats[j]);
    }
    writeString(row_format.delimiters[columns], out);
}

void TemplateBlockOutputFormat::serializeField(const IColumn & column, const ISerialization & serialization, size_t row_num, ColumnFormat col_format)
{
    switch (col_format)
    {
        case ColumnFormat::Escaped:
            serialization.serializeTextEscaped(column, row_num, out, settings);
            break;
        case ColumnFormat::Quoted:
            serialization.serializeTextQuoted(column, row_num, out, settings);
            break;
        case ColumnFormat::Csv:
            serialization.serializeTextCSV(column, row_num, out, settings);
            break;
        case ColumnFormat::Json:
            serialization.serializeTextJSON(column, row_num, out, settings);
            break;
        case ColumnFormat::Xml:
            serialization.serializeTextXML(column, row_num, out, settings);
            break;
        case ColumnFormat::Raw:
            serialization.serializeTextRaw(column, row_num, out, settings);
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
    serializeField(*col, *type->getDefaultSerialization(), 0, col_format);
}

void TemplateBlockOutputFormat::consume(Chunk chunk)
{
    doWritePrefix();

    size_t rows = chunk.getNumRows();

    for (size_t i = 0; i < rows; ++i)
    {
        if (row_count)
            writeString(row_between_delimiter, out);

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
        switch (static_cast<ResultsetPart>(*format.format_idx_to_column_idx[i]))
        {
            case ResultsetPart::Totals:
                if (!totals || !totals.hasRows())
                    format.throwInvalidFormat("Cannot print totals for this request", i);
                writeRow(totals, 0);
                break;
            case ResultsetPart::ExtremesMin:
                if (!extremes)
                    format.throwInvalidFormat("Cannot print extremes for this request", i);
                writeRow(extremes, 0);
                break;
            case ResultsetPart::ExtremesMax:
                if (!extremes)
                    format.throwInvalidFormat("Cannot print extremes for this request", i);
                writeRow(extremes, 1);
                break;
            case ResultsetPart::Rows:
                writeValue<size_t, DataTypeUInt64>(row_count, format.formats[i]);
                break;
            case ResultsetPart::RowsBeforeLimit:
                if (!rows_before_limit_set)
                    format.throwInvalidFormat("Cannot print rows_before_limit for this request", i);
                writeValue<size_t, DataTypeUInt64>(rows_before_limit, format.formats[i]);
                break;
            case ResultsetPart::TimeElapsed:
                writeValue<double, DataTypeFloat64>(watch.elapsedSeconds(), format.formats[i]);
                break;
            case ResultsetPart::RowsRead:
                writeValue<size_t, DataTypeUInt64>(progress.read_rows.load(), format.formats[i]);
                break;
            case ResultsetPart::BytesRead:
                writeValue<size_t, DataTypeUInt64>(progress.read_bytes.load(), format.formats[i]);
                break;
            default:
                break;
        }
        writeString(format.delimiters[i + 1], out);
    }

    finalized = true;
}


void registerOutputFormatTemplate(FormatFactory & factory)
{
    factory.registerOutputFormat("Template", [](
            WriteBuffer & buf,
            const Block & sample,
            const RowOutputFormatParams &,
            const FormatSettings & settings)
    {
        ParsedTemplateFormatString resultset_format;
        if (settings.template_settings.resultset_format.empty())
        {
            /// Default format string: "${data}"
            resultset_format.delimiters.resize(2);
            resultset_format.formats.emplace_back(ParsedTemplateFormatString::ColumnFormat::None);
            resultset_format.format_idx_to_column_idx.emplace_back(0);
            resultset_format.column_names.emplace_back("data");
        }
        else
        {
            /// Read format string from file
            resultset_format = ParsedTemplateFormatString(
                    FormatSchemaInfo(settings.template_settings.resultset_format, "Template", false,
                            settings.schema.is_server, settings.schema.format_schema_path),
                    [&](const String & partName)
                    {
                        return static_cast<size_t>(TemplateBlockOutputFormat::stringToResultsetPart(partName));
                    });
        }

        ParsedTemplateFormatString row_format = ParsedTemplateFormatString(
                FormatSchemaInfo(settings.template_settings.row_format, "Template", false,
                        settings.schema.is_server, settings.schema.format_schema_path),
                [&](const String & colName)
                {
                    return sample.getPositionByName(colName);
                });

        return std::make_shared<TemplateBlockOutputFormat>(sample, buf, settings, resultset_format, row_format, settings.template_settings.row_between_delimiter);
    });

    factory.registerOutputFormat("CustomSeparated", [](
            WriteBuffer & buf,
            const Block & sample,
            const RowOutputFormatParams &,
            const FormatSettings & settings)
    {
        ParsedTemplateFormatString resultset_format = ParsedTemplateFormatString::setupCustomSeparatedResultsetFormat(settings.custom);
        ParsedTemplateFormatString row_format = ParsedTemplateFormatString::setupCustomSeparatedRowFormat(settings.custom, sample);

        return std::make_shared<TemplateBlockOutputFormat>(sample, buf, settings, resultset_format, row_format, settings.custom.row_between_delimiter);
    });
}
}
