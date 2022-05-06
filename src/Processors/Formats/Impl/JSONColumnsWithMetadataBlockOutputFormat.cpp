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
    : JSONColumnsBlockOutputFormat(out_, header_, format_settings_, true, 1)
{
    bool need_validate_utf8 = false;
    makeNamesAndTypesWithValidUTF8(fields, format_settings, need_validate_utf8);

    if (need_validate_utf8)
    {
        validating_ostr = std::make_unique<WriteBufferValidUTF8>(out);
        ostr = validating_ostr.get();
    }
}

void JSONColumnsWithMetadataBlockOutputFormat::writePrefix()
{
    writeJSONObjectStart(*ostr);
    writeJSONMetadata(fields, format_settings, *ostr);
}

void JSONColumnsWithMetadataBlockOutputFormat::writeChunkStart()
{
    writeJSONFieldDelimiter(*ostr, 2);
    writeJSONObjectStart(*ostr, 1, "data");
}

void JSONColumnsWithMetadataBlockOutputFormat::writeChunkEnd()
{
    writeJSONObjectEnd(*ostr, indent);
}

void JSONColumnsWithMetadataBlockOutputFormat::consumeExtremes(Chunk chunk)
{
    auto num_rows = chunk.getNumRows();
    if (num_rows != 2)
        throw Exception("Got " + toString(num_rows) + " in extremes chunk, expected 2", ErrorCodes::LOGICAL_ERROR);

    const auto & columns = chunk.getColumns();
    writeJSONFieldDelimiter(*ostr, 2);
    writeJSONObjectStart(*ostr, 1, "extremes");
    writeExtremesElement("min", columns, 0);
    writeJSONFieldDelimiter(*ostr);
    writeExtremesElement("max", columns, 1);
    writeJSONObjectEnd(*ostr, 1);
}

void JSONColumnsWithMetadataBlockOutputFormat::writeExtremesElement(const char * title, const Columns & columns, size_t row_num)
{
    writeJSONObjectStart(*ostr, 2, title);
    writeJSONColumns(columns, fields, serializations, row_num, false, format_settings, *ostr, 3);
    writeJSONObjectEnd(*ostr, 2);
}

void JSONColumnsWithMetadataBlockOutputFormat::consumeTotals(Chunk chunk)
{
    auto num_rows = chunk.getNumRows();
    if (num_rows != 1)
        throw Exception("Got " + toString(num_rows) + " in totals chunk, expected 1", ErrorCodes::LOGICAL_ERROR);

    const auto & columns = chunk.getColumns();
    writeJSONFieldDelimiter(*ostr, 2);
    writeJSONObjectStart(*ostr, 1, "totals");
    writeJSONColumns(columns, fields, serializations, 0, false, format_settings, *ostr, 2);
    writeJSONObjectEnd(*ostr, 1);
}

void JSONColumnsWithMetadataBlockOutputFormat::finalizeImpl()
{
    auto outside_statistics = getOutsideStatistics();
    if (outside_statistics)
        statistics = std::move(*outside_statistics);

    writeJSONAdditionalInfo(
        total_rows_in_mono_block,
        statistics.rows_before_limit,
        statistics.applied_limit,
        statistics.watch,
        statistics.progress,
        format_settings.write_statistics,
        *ostr);

    writeJSONObjectEnd(*ostr);
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
