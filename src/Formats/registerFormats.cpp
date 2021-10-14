#if !defined(ARCADIA_BUILD)
#    include <Common/config.h>
#endif

#include <Formats/FormatFactory.h>


namespace DB
{

/// File Segmentation Engines for parallel reading

void registerFileSegmentationEngineTabSeparated(FormatFactory & factory);
void registerFileSegmentationEngineCSV(FormatFactory & factory);
void registerFileSegmentationEngineJSONEachRow(FormatFactory & factory);
void registerFileSegmentationEngineRegexp(FormatFactory & factory);
void registerFileSegmentationEngineJSONAsString(FormatFactory & factory);

/// Formats for both input/output.

void registerInputFormatNative(FormatFactory & factory);
void registerOutputFormatNative(FormatFactory & factory);

void registerInputFormatRowBinary(FormatFactory & factory);
void registerOutputFormatRowBinary(FormatFactory & factory);
void registerInputFormatTabSeparated(FormatFactory & factory);
void registerOutputFormatTabSeparated(FormatFactory & factory);
void registerInputFormatValues(FormatFactory & factory);
void registerOutputFormatValues(FormatFactory & factory);
void registerInputFormatCSV(FormatFactory & factory);
void registerOutputFormatCSV(FormatFactory & factory);
void registerInputFormatTSKV(FormatFactory & factory);
void registerOutputFormatTSKV(FormatFactory & factory);
void registerInputFormatJSONEachRow(FormatFactory & factory);
void registerOutputFormatJSONEachRow(FormatFactory & factory);
void registerInputFormatJSONCompactEachRow(FormatFactory & factory);
void registerOutputFormatJSONCompactEachRow(FormatFactory & factory);
void registerInputFormatProtobuf(FormatFactory & factory);
void registerOutputFormatProtobuf(FormatFactory & factory);
void registerInputFormatTemplate(FormatFactory & factory);
void registerOutputFormatTemplate(FormatFactory & factory);
void registerInputFormatMsgPack(FormatFactory & factory);
void registerOutputFormatMsgPack(FormatFactory & factory);
void registerInputFormatORC(FormatFactory & factory);
void registerOutputFormatORC(FormatFactory & factory);
void registerInputFormatParquet(FormatFactory & factory);
void registerOutputFormatParquet(FormatFactory & factory);
void registerInputFormatArrow(FormatFactory & factory);
void registerOutputFormatArrow(FormatFactory & factory);
void registerInputFormatAvro(FormatFactory & factory);
void registerOutputFormatAvro(FormatFactory & factory);
void registerInputFormatRawBLOB(FormatFactory & factory);
void registerOutputFormatRawBLOB(FormatFactory & factory);

/// Output only (presentational) formats.

void registerOutputFormatPretty(FormatFactory & factory);
void registerOutputFormatPrettyCompact(FormatFactory & factory);
void registerOutputFormatPrettySpace(FormatFactory & factory);
void registerOutputFormatVertical(FormatFactory & factory);
void registerOutputFormatJSON(FormatFactory & factory);
void registerOutputFormatJSONCompact(FormatFactory & factory);
void registerOutputFormatJSONEachRowWithProgress(FormatFactory & factory);
void registerOutputFormatXML(FormatFactory & factory);
void registerOutputFormatODBCDriver2(FormatFactory & factory);
void registerOutputFormatNull(FormatFactory & factory);
void registerOutputFormatMySQLWire(FormatFactory & factory);
void registerOutputFormatMarkdown(FormatFactory & factory);
void registerOutputFormatPostgreSQLWire(FormatFactory & factory);

/// Input only formats.

void registerInputFormatRegexp(FormatFactory & factory);
void registerInputFormatJSONAsString(FormatFactory & factory);
void registerInputFormatLineAsString(FormatFactory & factory);
void registerInputFormatCapnProto(FormatFactory & factory);

/// Non trivial prefix and suffix checkers for disabling parallel parsing.
void registerNonTrivialPrefixAndSuffixCheckerJSONEachRow(FormatFactory & factory);
void registerNonTrivialPrefixAndSuffixCheckerJSONAsString(FormatFactory & factory);

void registerFormats()
{
    auto & factory = FormatFactory::instance();

    registerFileSegmentationEngineTabSeparated(factory);
    registerFileSegmentationEngineCSV(factory);
    registerFileSegmentationEngineJSONEachRow(factory);
    registerFileSegmentationEngineRegexp(factory);
    registerFileSegmentationEngineJSONAsString(factory);

    registerInputFormatNative(factory);
    registerOutputFormatNative(factory);

    registerInputFormatRowBinary(factory);
    registerOutputFormatRowBinary(factory);
    registerInputFormatTabSeparated(factory);
    registerOutputFormatTabSeparated(factory);
    registerInputFormatValues(factory);
    registerOutputFormatValues(factory);
    registerInputFormatCSV(factory);
    registerOutputFormatCSV(factory);
    registerInputFormatTSKV(factory);
    registerOutputFormatTSKV(factory);
    registerInputFormatJSONEachRow(factory);
    registerOutputFormatJSONEachRow(factory);
    registerInputFormatJSONCompactEachRow(factory);
    registerOutputFormatJSONCompactEachRow(factory);
    registerInputFormatProtobuf(factory);
    registerOutputFormatProtobuf(factory);
    registerInputFormatTemplate(factory);
    registerOutputFormatTemplate(factory);
    registerInputFormatMsgPack(factory);
    registerOutputFormatMsgPack(factory);
    registerInputFormatRawBLOB(factory);
    registerOutputFormatRawBLOB(factory);

    registerInputFormatORC(factory);
    registerOutputFormatORC(factory);
    registerInputFormatParquet(factory);
    registerOutputFormatParquet(factory);
#if !defined(ARCADIA_BUILD)
    registerInputFormatAvro(factory);
    registerOutputFormatAvro(factory);
#endif
    registerInputFormatArrow(factory);
    registerOutputFormatArrow(factory);

    registerOutputFormatPretty(factory);
    registerOutputFormatPrettyCompact(factory);
    registerOutputFormatPrettySpace(factory);
    registerOutputFormatVertical(factory);
    registerOutputFormatJSON(factory);
    registerOutputFormatJSONCompact(factory);
    registerOutputFormatJSONEachRowWithProgress(factory);
    registerOutputFormatXML(factory);
    registerOutputFormatODBCDriver2(factory);
    registerOutputFormatNull(factory);
    registerOutputFormatMySQLWire(factory);
    registerOutputFormatMarkdown(factory);
    registerOutputFormatPostgreSQLWire(factory);

    registerInputFormatRegexp(factory);
    registerInputFormatJSONAsString(factory);
    registerInputFormatLineAsString(factory);

#if !defined(ARCADIA_BUILD)
    registerInputFormatCapnProto(factory);
#endif

    registerNonTrivialPrefixAndSuffixCheckerJSONEachRow(factory);
    registerNonTrivialPrefixAndSuffixCheckerJSONAsString(factory);
}

}

