#include <Processors/Formats/Impl/JSONColumnsWithMetadataBlockOutputFormat.h>
#include <Formats/JSONUtils.h>
#include <Formats/FormatFactory.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferValidUTF8.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

JSONColumnsWithMetadataBlockOutputFormat::JSONColumnsWithMetadataBlockOutputFormat(WriteBuffer & out_, const Block & header_, const FormatSettings & format_settings_)
    : JSONColumnsBlockOutputFormat(out_, header_, format_settings_, 1)
{
    bool need_validate_utf8 = false;
    JSONUtils::makeNamesAndTypesWithValidUTF8(fields, format_settings, need_validate_utf8);

    if (need_validate_utf8)
    {
        validating_ostr = std::make_unique<WriteBufferValidUTF8>(out);
        ostr = validating_ostr.get();
    }
}

void JSONColumnsWithMetadataBlockOutputFormat::writePrefix()
{
    JSONUtils::writeObjectStart(*ostr);
    JSONUtils::writeMetadata(fields, format_settings, *ostr);
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
        throw Exception("Got " + toString(num_rows) + " in extremes chunk, expected 2", ErrorCodes::LOGICAL_ERROR);

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
    JSONUtils::writeColumns(columns, fields, serializations, row_num, false, format_settings, *ostr, 3);
    JSONUtils::writeObjectEnd(*ostr, 2);
}

void JSONColumnsWithMetadataBlockOutputFormat::consumeTotals(Chunk chunk)
{
    auto num_rows = chunk.getNumRows();
    if (num_rows != 1)
        throw Exception("Got " + toString(num_rows) + " in totals chunk, expected 1", ErrorCodes::LOGICAL_ERROR);

    const auto & columns = chunk.getColumns();
    JSONUtils::writeFieldDelimiter(*ostr, 2);
    JSONUtils::writeObjectStart(*ostr, 1, "totals");
    JSONUtils::writeColumns(columns, fields, serializations, 0, false, format_settings, *ostr, 2);
    JSONUtils::writeObjectEnd(*ostr, 1);
}

void JSONColumnsWithMetadataBlockOutputFormat::finalizeImpl()
{
    auto outside_statistics = getOutsideStatistics();
    if (outside_statistics)
        statistics = std::move(*outside_statistics);

    JSONUtils::writeAdditionalInfo(
        rows,
        statistics.rows_before_limit,
        statistics.applied_limit,
        statistics.watch,
        statistics.progress,
        format_settings.write_statistics,
        *ostr);

    JSONUtils::writeObjectEnd(*ostr);
    writeChar('\n', *ostr);
    ostr->next();
}

void registerOutputFormatJSONColumnsWithMetadata(FormatFactory & factory)
{
    factory.registerOutputFormat("JSONColumnsWithMetadata", [](
        WriteBuffer & buf,
        const Block & sample,
        const RowOutputFormatParams &,
        const FormatSettings & format_settings)
    {
        return std::make_shared<JSONColumnsWithMetadataBlockOutputFormat>(buf, sample, format_settings);
    });

    factory.markFormatHasNoAppendSupport("JSONColumnsWithMetadata");
}

}
