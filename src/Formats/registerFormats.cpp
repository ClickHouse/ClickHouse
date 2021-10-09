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

void registerInputFormatProcessorNative(FormatFactory & factory);
void registerOutputFormatProcessorNative(FormatFactory & factory);
void registerInputFormatProcessorRowBinary(FormatFactory & factory);
void registerOutputFormatProcessorRowBinary(FormatFactory & factory);
void registerInputFormatProcessorTabSeparated(FormatFactory & factory);
void registerOutputFormatProcessorTabSeparated(FormatFactory & factory);
void registerInputFormatProcessorValues(FormatFactory & factory);
void registerOutputFormatProcessorValues(FormatFactory & factory);
void registerInputFormatProcessorCSV(FormatFactory & factory);
void registerOutputFormatProcessorCSV(FormatFactory & factory);
void registerInputFormatProcessorTSKV(FormatFactory & factory);
void registerOutputFormatProcessorTSKV(FormatFactory & factory);
void registerInputFormatProcessorJSONEachRow(FormatFactory & factory);
void registerOutputFormatProcessorJSONEachRow(FormatFactory & factory);
void registerInputFormatProcessorJSONCompactEachRow(FormatFactory & factory);
void registerOutputFormatProcessorJSONCompactEachRow(FormatFactory & factory);
void registerInputFormatProcessorProtobuf(FormatFactory & factory);
void registerOutputFormatProcessorProtobuf(FormatFactory & factory);
void registerInputFormatProcessorTemplate(FormatFactory & factory);
void registerOutputFormatProcessorTemplate(FormatFactory & factory);
void registerInputFormatProcessorMsgPack(FormatFactory & factory);
void registerOutputFormatProcessorMsgPack(FormatFactory & factory);
void registerInputFormatProcessorORC(FormatFactory & factory);
void registerOutputFormatProcessorORC(FormatFactory & factory);
void registerInputFormatProcessorParquet(FormatFactory & factory);
void registerOutputFormatProcessorParquet(FormatFactory & factory);
void registerInputFormatProcessorArrow(FormatFactory & factory);
void registerOutputFormatProcessorArrow(FormatFactory & factory);
void registerInputFormatProcessorAvro(FormatFactory & factory);
void registerOutputFormatProcessorAvro(FormatFactory & factory);
void registerInputFormatProcessorRawBLOB(FormatFactory & factory);
void registerOutputFormatProcessorRawBLOB(FormatFactory & factory);

/// Output only (presentational) formats.

void registerOutputFormatNull(FormatFactory & factory);

void registerOutputFormatProcessorPretty(FormatFactory & factory);
void registerOutputFormatProcessorPrettyCompact(FormatFactory & factory);
void registerOutputFormatProcessorPrettySpace(FormatFactory & factory);
void registerOutputFormatProcessorVertical(FormatFactory & factory);
void registerOutputFormatProcessorJSON(FormatFactory & factory);
void registerOutputFormatProcessorJSONCompact(FormatFactory & factory);
void registerOutputFormatProcessorJSONEachRowWithProgress(FormatFactory & factory);
void registerOutputFormatProcessorXML(FormatFactory & factory);
void registerOutputFormatProcessorODBCDriver2(FormatFactory & factory);
void registerOutputFormatProcessorNull(FormatFactory & factory);
void registerOutputFormatProcessorMySQLWire(FormatFactory & factory);
void registerOutputFormatProcessorMarkdown(FormatFactory & factory);
void registerOutputFormatProcessorPostgreSQLWire(FormatFactory & factory);

/// Input only formats.

void registerInputFormatProcessorRegexp(FormatFactory & factory);
void registerInputFormatProcessorJSONAsString(FormatFactory & factory);
void registerInputFormatProcessorLineAsString(FormatFactory & factory);
void registerInputFormatProcessorCapnProto(FormatFactory & factory);


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

    registerInputFormatProcessorNative(factory);
    registerOutputFormatProcessorNative(factory);
    registerInputFormatProcessorRowBinary(factory);
    registerOutputFormatProcessorRowBinary(factory);
    registerInputFormatProcessorTabSeparated(factory);
    registerOutputFormatProcessorTabSeparated(factory);
    registerInputFormatProcessorValues(factory);
    registerOutputFormatProcessorValues(factory);
    registerInputFormatProcessorCSV(factory);
    registerOutputFormatProcessorCSV(factory);
    registerInputFormatProcessorTSKV(factory);
    registerOutputFormatProcessorTSKV(factory);
    registerInputFormatProcessorJSONEachRow(factory);
    registerOutputFormatProcessorJSONEachRow(factory);
    registerInputFormatProcessorJSONCompactEachRow(factory);
    registerOutputFormatProcessorJSONCompactEachRow(factory);
    registerInputFormatProcessorProtobuf(factory);
    registerOutputFormatProcessorProtobuf(factory);
    registerInputFormatProcessorTemplate(factory);
    registerOutputFormatProcessorTemplate(factory);
    registerInputFormatProcessorMsgPack(factory);
    registerOutputFormatProcessorMsgPack(factory);
    registerInputFormatProcessorRawBLOB(factory);
    registerOutputFormatProcessorRawBLOB(factory);

#if !defined(ARCADIA_BUILD)
    registerInputFormatProcessorORC(factory);
    registerOutputFormatProcessorORC(factory);
    registerInputFormatProcessorParquet(factory);
    registerOutputFormatProcessorParquet(factory);
    registerInputFormatProcessorArrow(factory);
    registerOutputFormatProcessorArrow(factory);
    registerInputFormatProcessorAvro(factory);
    registerOutputFormatProcessorAvro(factory);
#endif

    registerOutputFormatNull(factory);

    registerOutputFormatProcessorPretty(factory);
    registerOutputFormatProcessorPrettyCompact(factory);
    registerOutputFormatProcessorPrettySpace(factory);
    registerOutputFormatProcessorVertical(factory);
    registerOutputFormatProcessorJSON(factory);
    registerOutputFormatProcessorJSONCompact(factory);
    registerOutputFormatProcessorJSONEachRowWithProgress(factory);
    registerOutputFormatProcessorXML(factory);
    registerOutputFormatProcessorODBCDriver2(factory);
    registerOutputFormatProcessorNull(factory);
    registerOutputFormatProcessorMySQLWire(factory);
    registerOutputFormatProcessorMarkdown(factory);
    registerOutputFormatProcessorPostgreSQLWire(factory);

    registerInputFormatProcessorRegexp(factory);
    registerInputFormatProcessorJSONAsString(factory);
    registerInputFormatProcessorLineAsString(factory);

#if !defined(ARCADIA_BUILD)
    registerInputFormatProcessorCapnProto(factory);
#endif
}

}

