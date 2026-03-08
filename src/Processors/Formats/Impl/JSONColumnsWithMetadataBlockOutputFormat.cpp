#include <Processors/Formats/Impl/JSONColumnsWithMetadataBlockOutputFormat.h>
#include <Formats/JSONUtils.h>
#include <Formats/FormatFactory.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferValidUTF8.h>
#include <Processors/Port.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

JSONColumnsWithMetadataBlockOutputFormat::JSONColumnsWithMetadataBlockOutputFormat(WriteBuffer & out_, SharedHeader header_, const FormatSettings & format_settings_)
    : JSONColumnsBlockOutputFormat(out_, header_, format_settings_, true, 1), types(header_->getDataTypes())
{
}

void JSONColumnsWithMetadataBlockOutputFormat::writePrefix()
{
    JSONUtils::writeObjectStart(*ostr);
    JSONUtils::writeMetadata(names, types, format_settings, *ostr);
}

void JSONColumnsWithMetadataBlockOutputFormat::writeSuffix()
{
    rows = mono_chunk.getNumRows();
    JSONColumnsBlockOutputFormatBase::writeSuffix();
}

void JSONColumnsWithMetadataBlockOutputFormat::writeChunkStart()
{
    JSONUtils::writeFieldDelimiter(*ostr, 2);
    JSONUtils::writeObjectStart(*ostr, 1, "data");
}

void JSONColumnsWithMetadataBlockOutputFormat::writeChunkEnd()
{
    JSONUtils::writeObjectEnd(*ostr, indent);
}

void JSONColumnsWithMetadataBlockOutputFormat::consumeExtremes(Chunk chunk)
{
    auto num_rows = chunk.getNumRows();
    if (num_rows != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Got {} in extremes chunk, expected 2", num_rows);

    const auto & columns = chunk.getColumns();
    JSONUtils::writeFieldDelimiter(*ostr, 2);
    JSONUtils::writeObjectStart(*ostr, 1, "extremes");
    writeExtremesElement("min", columns, 0);
    JSONUtils::writeFieldDelimiter(*ostr);
    writeExtremesElement("max", columns, 1);
    JSONUtils::writeObjectEnd(*ostr, 1);
}

void JSONColumnsWithMetadataBlockOutputFormat::writeExtremesElement(const char * title, const Columns & columns, size_t row_num)
{
    JSONUtils::writeObjectStart(*ostr, 2, title);
    JSONUtils::writeColumns(columns, names, serializations, row_num, false, format_settings, *ostr, 3);
    JSONUtils::writeObjectEnd(*ostr, 2);
}

void JSONColumnsWithMetadataBlockOutputFormat::consumeTotals(Chunk chunk)
{
    auto num_rows = chunk.getNumRows();
    if (num_rows != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Got {} in totals chunk, expected 1", num_rows);

    const auto & columns = chunk.getColumns();
    JSONUtils::writeFieldDelimiter(*ostr, 2);
    JSONUtils::writeObjectStart(*ostr, 1, "totals");
    JSONUtils::writeColumns(columns, names, serializations, 0, false, format_settings, *ostr, 2);
    JSONUtils::writeObjectEnd(*ostr, 1);
}

void JSONColumnsWithMetadataBlockOutputFormat::finalizeImpl()
{
    JSONUtils::writeAdditionalInfo(
        rows,
        statistics.rows_before_limit,
        statistics.applied_limit,
        statistics.rows_before_aggregation,
        statistics.applied_aggregation,
        statistics.watch,
        statistics.progress,
        format_settings.write_statistics && !hasDeferredStatistics(),
        *ostr);

    /// When statistics are deferred, skip closing the document here.
    /// It will be done in writeDeferredStatisticsAndFinalize().
    if (hasDeferredStatistics())
        return;

    JSONUtils::writeObjectEnd(*ostr);
    writeChar('\n', *ostr);
    ostr->next();
}

bool JSONColumnsWithMetadataBlockOutputFormat::hasDeferredStatistics() const
{
    return format_settings.write_statistics;
}

void JSONColumnsWithMetadataBlockOutputFormat::writeDeferredStatisticsAndFinalize()
{
    JSONUtils::writeFieldDelimiter(*ostr, 2);
    JSONUtils::writeObjectStart(*ostr, 1, "statistics");

    writeCString("\t\t\"elapsed\": ", *ostr);
    writeText(statistics.watch.elapsedSeconds(), *ostr);
    JSONUtils::writeFieldDelimiter(*ostr);

    writeCString("\t\t\"rows_read\": ", *ostr);
    writeText(statistics.progress.read_rows.load(), *ostr);
    JSONUtils::writeFieldDelimiter(*ostr);

    writeCString("\t\t\"bytes_read\": ", *ostr);
    writeText(statistics.progress.read_bytes.load(), *ostr);

    JSONUtils::writeObjectEnd(*ostr, 1);

    JSONUtils::writeObjectEnd(*ostr);
    writeChar('\n', *ostr);
    ostr->next();
}

void JSONColumnsWithMetadataBlockOutputFormat::resetFormatterImpl()
{
    JSONColumnsBlockOutputFormat::resetFormatterImpl();
    rows = 0;
    statistics = Statistics();
}

void registerOutputFormatJSONColumnsWithMetadata(FormatFactory & factory)
{
    factory.registerOutputFormat("JSONColumnsWithMetadata", [](
        WriteBuffer & buf,
        const Block & sample,
        const FormatSettings & format_settings,
        FormatFilterInfoPtr /*format_filter_info*/)
    {
        return std::make_shared<JSONColumnsWithMetadataBlockOutputFormat>(buf, std::make_shared<const Block>(sample), format_settings);
    });

    factory.markFormatHasNoAppendSupport("JSONColumnsWithMetadata");
    factory.setContentType("JSONColumnsWithMetadata", "application/json; charset=UTF-8");
}

}
