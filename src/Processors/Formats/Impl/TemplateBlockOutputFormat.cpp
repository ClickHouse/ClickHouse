#include <Processors/Formats/Impl/TemplateBlockOutputFormat.h>
#include <Formats/FormatFactory.h>
#include <Formats/EscapingRuleUtils.h>
#include <IO/WriteHelpers.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
    extern const int INVALID_TEMPLATE_FORMAT;
}

TemplateBlockOutputFormat::TemplateBlockOutputFormat(const Block & header_, WriteBuffer & out_, const FormatSettings & settings_,
                                                     ParsedTemplateFormatString format_, ParsedTemplateFormatString row_format_,
                                                     std::string row_between_delimiter_)
    : IOutputFormat(header_, out_), settings(settings_), serializations(header_.getSerializations()), format(std::move(format_))
    , row_format(std::move(row_format_)), row_between_delimiter(std::move(row_between_delimiter_))
{
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
                if (format.escaping_rules[i] != EscapingRule::None)
                    format.throwInvalidFormat("Serialization type for data, totals, min and max must be empty or None", i);
                break;
            case static_cast<size_t>(ResultsetPart::Rows):
            case static_cast<size_t>(ResultsetPart::RowsBeforeLimit):
            case static_cast<size_t>(ResultsetPart::TimeElapsed):
            case static_cast<size_t>(ResultsetPart::RowsRead):
            case static_cast<size_t>(ResultsetPart::BytesRead):
            case static_cast<size_t>(ResultsetPart::RowsBeforeAggregation):
                if (format.escaping_rules[i] == EscapingRule::None)
                    format.throwInvalidFormat(
                        "Serialization type for output part rows, rows, time, "
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
        if (row_format.escaping_rules[i] == EscapingRule::None)
            row_format.throwInvalidFormat("Serialization type for file column is not specified", i);
    }
}

TemplateBlockOutputFormat::ResultsetPart TemplateBlockOutputFormat::stringToResultsetPart(const String & part)
{
    if (part == "data")
        return ResultsetPart::Data;
    if (part == "totals")
        return ResultsetPart::Totals;
    if (part == "min")
        return ResultsetPart::ExtremesMin;
    if (part == "max")
        return ResultsetPart::ExtremesMax;
    if (part == "rows")
        return ResultsetPart::Rows;
    if (part == "rows_before_limit")
        return ResultsetPart::RowsBeforeLimit;
    if (part == "time")
        return ResultsetPart::TimeElapsed;
    if (part == "rows_read")
        return ResultsetPart::RowsRead;
    if (part == "bytes_read")
        return ResultsetPart::BytesRead;
    if (part == "rows_before_aggregation")
        return ResultsetPart::RowsBeforeAggregation;
    throw Exception(ErrorCodes::SYNTAX_ERROR, "Unknown output part {}", part);
}

void TemplateBlockOutputFormat::writeRow(const Chunk & chunk, size_t row_num)
{
    size_t columns = row_format.format_idx_to_column_idx.size();
    for (size_t j = 0; j < columns; ++j)
    {
        writeString(row_format.delimiters[j], out);

        size_t col_idx = *row_format.format_idx_to_column_idx[j];
        serializeFieldByEscapingRule(*chunk.getColumns()[col_idx], *serializations[col_idx], out, row_num, row_format.escaping_rules[j], settings);
    }
    writeString(row_format.delimiters[columns], out);
}

template <typename U, typename V> void TemplateBlockOutputFormat::writeValue(U value, EscapingRule escaping_rule)
{
    auto type = std::make_unique<V>();
    auto col = type->createColumn();
    col->insert(value);
    serializeFieldByEscapingRule(*col, *type->getDefaultSerialization(), out, 0, escaping_rule, settings);
}

void TemplateBlockOutputFormat::consume(Chunk chunk)
{
    size_t rows = chunk.getNumRows();

    for (size_t i = 0; i < rows; ++i)
    {
        if (row_count)
            writeString(row_between_delimiter, out);

        writeRow(chunk, i);
        ++row_count;
    }
}

void TemplateBlockOutputFormat::writePrefix()
{
    writeString(format.delimiters.front(), out);
}

void TemplateBlockOutputFormat::finalizeImpl()
{
    size_t parts = format.format_idx_to_column_idx.size();
    for (size_t i = 0; i < parts; ++i)
    {
        auto type = std::make_shared<DataTypeUInt64>();
        ColumnWithTypeAndName col(type->createColumnConst(1, row_count), type, String("tmp"));
        switch (static_cast<ResultsetPart>(*format.format_idx_to_column_idx[i]))
        {
            case ResultsetPart::Totals:
                if (!statistics.totals || !statistics.totals.hasRows())
                    format.throwInvalidFormat("Cannot print totals for this request", i);
                writeRow(statistics.totals, 0);
                break;
            case ResultsetPart::ExtremesMin:
                if (!statistics.extremes)
                    format.throwInvalidFormat("Cannot print extremes for this request", i);
                writeRow(statistics.extremes, 0);
                break;
            case ResultsetPart::ExtremesMax:
                if (!statistics.extremes)
                    format.throwInvalidFormat("Cannot print extremes for this request", i);
                writeRow(statistics.extremes, 1);
                break;
            case ResultsetPart::Rows:
                writeValue<size_t, DataTypeUInt64>(row_count, format.escaping_rules[i]);
                break;
            case ResultsetPart::RowsBeforeLimit:
                if (!statistics.applied_limit)
                    format.throwInvalidFormat("Cannot print rows_before_limit for this request", i);
                writeValue<size_t, DataTypeUInt64>(statistics.rows_before_limit, format.escaping_rules[i]);
                break;
            case ResultsetPart::TimeElapsed:
                writeValue<double, DataTypeFloat64>(statistics.watch.elapsedSeconds(), format.escaping_rules[i]);
                break;
            case ResultsetPart::RowsRead:
                writeValue<size_t, DataTypeUInt64>(statistics.progress.read_rows.load(), format.escaping_rules[i]);
                break;
            case ResultsetPart::BytesRead:
                writeValue<size_t, DataTypeUInt64>(statistics.progress.read_bytes.load(), format.escaping_rules[i]);
                break;
            case ResultsetPart::RowsBeforeAggregation:
                if (!statistics.applied_aggregation)
                    format.throwInvalidFormat("Cannot print rows_before_aggregation for this request", i);
                writeValue<size_t, DataTypeUInt64>(statistics.rows_before_aggregation, format.escaping_rules[i]);
                break;
            default:
                break;
        }
        writeString(format.delimiters[i + 1], out);
    }
}

void TemplateBlockOutputFormat::resetFormatterImpl()
{
    row_count = 0;
    statistics = Statistics();
}

void registerOutputFormatTemplate(FormatFactory & factory)
{
    factory.registerOutputFormat("Template", [](
            WriteBuffer & buf,
            const Block & sample,
            const FormatSettings & settings)
    {
        ParsedTemplateFormatString resultset_format;
        auto idx_resultset_by_name = [&](const String & partName)
        {
            return static_cast<size_t>(TemplateBlockOutputFormat::stringToResultsetPart(partName));
        };
        if (settings.template_settings.resultset_format.empty())
        {
            /// Default format string: "${data}"
            if (settings.template_settings.resultset_format_template.empty())
            {
                resultset_format.delimiters.resize(2);
                resultset_format.escaping_rules.emplace_back(ParsedTemplateFormatString::EscapingRule::None);
                resultset_format.format_idx_to_column_idx.emplace_back(0);
                resultset_format.column_names.emplace_back("data");
            }
            else
            {
                resultset_format = ParsedTemplateFormatString();
                resultset_format.parse(settings.template_settings.resultset_format_template, idx_resultset_by_name);
            }
        }
        else
        {
            /// Read format string from file
            resultset_format = ParsedTemplateFormatString(
                    FormatSchemaInfo(settings.template_settings.resultset_format, "Template", false,
                            settings.schema.is_server, settings.schema.format_schema_path),
                    idx_resultset_by_name);
            if (!settings.template_settings.resultset_format_template.empty())
            {
                throw Exception(DB::ErrorCodes::INVALID_TEMPLATE_FORMAT, "Expected either format_template_resultset or format_template_resultset_format, but not both");
            }
        }

        ParsedTemplateFormatString row_format;
        auto idx_row_by_name = [&](const String & colName)
        {
            return sample.getPositionByName(colName);
        };
        if (settings.template_settings.row_format.empty())
        {
            row_format = ParsedTemplateFormatString();
            row_format.parse(settings.template_settings.row_format_template, idx_row_by_name);
        }
        else
        {
            row_format = ParsedTemplateFormatString(
                FormatSchemaInfo(settings.template_settings.row_format, "Template", false,
                        settings.schema.is_server, settings.schema.format_schema_path),
                idx_row_by_name);
            if (!settings.template_settings.row_format_template.empty())
            {
                throw Exception(DB::ErrorCodes::INVALID_TEMPLATE_FORMAT, "Expected either format_template_row or format_template_row_format, but not both");
            }
        }
        return std::make_shared<TemplateBlockOutputFormat>(sample, buf, settings, resultset_format, row_format, settings.template_settings.row_between_delimiter);
    });

    factory.registerAppendSupportChecker("Template", [](const FormatSettings & settings)
    {
        if (settings.template_settings.resultset_format.empty())
            return true;
        auto resultset_format = ParsedTemplateFormatString(
            FormatSchemaInfo(settings.template_settings.resultset_format, "Template", false,
                             settings.schema.is_server, settings.schema.format_schema_path),
            [&](const String & partName)
            {
                return static_cast<size_t>(TemplateBlockOutputFormat::stringToResultsetPart(partName));
            });
        return resultset_format.delimiters.empty() || resultset_format.delimiters.back().empty();
    });
}
}
