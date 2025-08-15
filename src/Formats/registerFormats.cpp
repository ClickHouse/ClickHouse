#include "config.h"

#include <Formats/FormatFactory.h>


namespace DB
{

/// File Segmentation Engines for parallel reading

void registerFileSegmentationEngineTabSeparated(FormatFactory & factory);
void registerFileSegmentationEngineCSV(FormatFactory & factory);
void registerFileSegmentationEngineJSONEachRow(FormatFactory & factory);
void registerFileSegmentationEngineRegexp(FormatFactory & factory);
void registerFileSegmentationEngineJSONAsString(FormatFactory & factory);
void registerFileSegmentationEngineJSONAsObject(FormatFactory & factory);
void registerFileSegmentationEngineJSONCompactEachRow(FormatFactory & factory);
#if USE_HIVE
void registerFileSegmentationEngineHiveText(FormatFactory & factory);
#endif
void registerFileSegmentationEngineLineAsString(FormatFactory & factory);
void registerFileSegmentationEngineBSONEachRow(FormatFactory & factory);

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
void registerOutputFormatJSON(FormatFactory & factory);
void registerInputFormatJSON(FormatFactory & factory);
void registerOutputFormatJSONCompact(FormatFactory & factory);
void registerInputFormatJSONCompact(FormatFactory & factory);
void registerInputFormatJSONEachRow(FormatFactory & factory);
void registerOutputFormatJSONEachRow(FormatFactory & factory);
void registerInputFormatJSONObjectEachRow(FormatFactory & factory);
void registerOutputFormatJSONObjectEachRow(FormatFactory & factory);
void registerInputFormatJSONCompactEachRow(FormatFactory & factory);
void registerOutputFormatJSONCompactEachRow(FormatFactory & factory);
void registerInputFormatJSONColumns(FormatFactory & factory);
void registerOutputFormatJSONColumns(FormatFactory & factory);
void registerInputFormatJSONCompactColumns(FormatFactory & factory);
void registerOutputFormatJSONCompactColumns(FormatFactory & factory);
void registerInputFormatBSONEachRow(FormatFactory & factory);
void registerOutputFormatBSONEachRow(FormatFactory & factory);
void registerInputFormatJSONColumnsWithMetadata(FormatFactory & factory);
void registerOutputFormatJSONColumnsWithMetadata(FormatFactory & factory);
void registerInputFormatProtobuf(FormatFactory & factory);
void registerOutputFormatProtobuf(FormatFactory & factory);
void registerInputFormatProtobufList(FormatFactory & factory);
void registerOutputFormatProtobufList(FormatFactory & factory);
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
void registerInputFormatCustomSeparated(FormatFactory & factory);
void registerOutputFormatCustomSeparated(FormatFactory & factory);
void registerInputFormatCapnProto(FormatFactory & factory);
void registerOutputFormatCapnProto(FormatFactory & factory);
void registerInputFormatNpy(FormatFactory & factory);
void registerOutputFormatNpy(FormatFactory & factory);
void registerInputFormatForm(FormatFactory & factory);

/// Output only (presentational) formats.

void registerOutputFormatPretty(FormatFactory & factory);
void registerOutputFormatPrettyCompact(FormatFactory & factory);
void registerOutputFormatPrettySpace(FormatFactory & factory);
void registerOutputFormatVertical(FormatFactory & factory);
void registerOutputFormatJSONEachRowWithProgress(FormatFactory & factory);
void registerOutputFormatXML(FormatFactory & factory);
void registerOutputFormatODBCDriver2(FormatFactory & factory);
void registerOutputFormatNull(FormatFactory & factory);
void registerOutputFormatMySQLWire(FormatFactory & factory);
void registerOutputFormatMarkdown(FormatFactory & factory);
void registerOutputFormatPostgreSQLWire(FormatFactory & factory);
void registerOutputFormatPrometheus(FormatFactory & factory);
void registerOutputFormatSQLInsert(FormatFactory & factory);

/// Input only formats.

void registerInputFormatRegexp(FormatFactory & factory);
void registerInputFormatJSONAsString(FormatFactory & factory);
void registerInputFormatJSONAsObject(FormatFactory & factory);
void registerInputFormatLineAsString(FormatFactory & factory);
void registerInputFormatMySQLDump(FormatFactory & factory);
void registerInputFormatParquetMetadata(FormatFactory & factory);
void registerInputFormatDWARF(FormatFactory & factory);
void registerInputFormatOne(FormatFactory & factory);

#if USE_HIVE
void registerInputFormatHiveText(FormatFactory & factory);
#endif

/// Non trivial prefix and suffix checkers for disabling parallel parsing.
void registerNonTrivialPrefixAndSuffixCheckerJSONEachRow(FormatFactory & factory);
void registerNonTrivialPrefixAndSuffixCheckerJSONAsString(FormatFactory & factory);
void registerNonTrivialPrefixAndSuffixCheckerJSONAsObject(FormatFactory & factory);

void registerArrowSchemaReader(FormatFactory & factory);
void registerParquetSchemaReader(FormatFactory & factory);
void registerORCSchemaReader(FormatFactory & factory);
void registerTSVSchemaReader(FormatFactory & factory);
void registerCSVSchemaReader(FormatFactory & factory);
void registerJSONCompactEachRowSchemaReader(FormatFactory & factory);
void registerJSONSchemaReader(FormatFactory & factory);
void registerJSONEachRowSchemaReader(FormatFactory & factory);
void registerJSONObjectEachRowSchemaReader(FormatFactory & factory);
void registerJSONAsStringSchemaReader(FormatFactory & factory);
void registerJSONAsObjectSchemaReader(FormatFactory & factory);
void registerJSONColumnsSchemaReader(FormatFactory & factory);
void registerJSONCompactColumnsSchemaReader(FormatFactory & factory);
void registerJSONColumnsWithMetadataSchemaReader(FormatFactory & factory);
void registerNativeSchemaReader(FormatFactory & factory);
void registerRowBinaryWithNamesAndTypesSchemaReader(FormatFactory & factory);
void registerAvroSchemaReader(FormatFactory & factory);
void registerProtobufSchemaReader(FormatFactory & factory);
void registerProtobufListSchemaReader(FormatFactory & factory);
void registerLineAsStringSchemaReader(FormatFactory & factory);
void registerRawBLOBSchemaReader(FormatFactory & factory);
void registerMsgPackSchemaReader(FormatFactory & factory);
void registerCapnProtoSchemaReader(FormatFactory & factory);
void registerCustomSeparatedSchemaReader(FormatFactory & factory);
void registerRegexpSchemaReader(FormatFactory & factory);
void registerTSKVSchemaReader(FormatFactory & factory);
void registerValuesSchemaReader(FormatFactory & factory);
void registerTemplateSchemaReader(FormatFactory & factory);
void registerMySQLSchemaReader(FormatFactory & factory);
void registerBSONEachRowSchemaReader(FormatFactory & factory);
void registerParquetMetadataSchemaReader(FormatFactory & factory);
void registerDWARFSchemaReader(FormatFactory & factory);
void registerOneSchemaReader(FormatFactory & factory);
void registerNpySchemaReader(FormatFactory & factory);
void registerFormSchemaReader(FormatFactory & factory);

void registerFileExtensions(FormatFactory & factory);

void registerFormats()
{
    auto & factory = FormatFactory::instance();

    registerFileSegmentationEngineTabSeparated(factory);
    registerFileSegmentationEngineCSV(factory);
    registerFileSegmentationEngineRegexp(factory);
    registerFileSegmentationEngineJSONEachRow(factory);
    registerFileSegmentationEngineJSONAsString(factory);
    registerFileSegmentationEngineJSONAsObject(factory);
    registerFileSegmentationEngineJSONCompactEachRow(factory);
#if USE_HIVE
    registerFileSegmentationEngineHiveText(factory);
#endif
    registerFileSegmentationEngineLineAsString(factory);
    registerFileSegmentationEngineBSONEachRow(factory);


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
    registerOutputFormatJSON(factory);
    registerInputFormatJSON(factory);
    registerOutputFormatJSONCompact(factory);
    registerInputFormatJSONCompact(factory);
    registerInputFormatJSONEachRow(factory);
    registerOutputFormatJSONEachRow(factory);
    registerInputFormatJSONObjectEachRow(factory);
    registerOutputFormatJSONObjectEachRow(factory);
    registerInputFormatJSONCompactEachRow(factory);
    registerOutputFormatJSONCompactEachRow(factory);
    registerInputFormatJSONColumns(factory);
    registerOutputFormatJSONColumns(factory);
    registerInputFormatJSONCompactColumns(factory);
    registerOutputFormatJSONCompactColumns(factory);
    registerInputFormatBSONEachRow(factory);
    registerOutputFormatBSONEachRow(factory);
    registerInputFormatJSONColumnsWithMetadata(factory);
    registerOutputFormatJSONColumnsWithMetadata(factory);
    registerInputFormatProtobuf(factory);
    registerOutputFormatProtobufList(factory);
    registerInputFormatProtobufList(factory);
    registerOutputFormatProtobuf(factory);
    registerInputFormatTemplate(factory);
    registerOutputFormatTemplate(factory);
    registerInputFormatMsgPack(factory);
    registerOutputFormatMsgPack(factory);
    registerInputFormatRawBLOB(factory);
    registerOutputFormatRawBLOB(factory);
    registerInputFormatCustomSeparated(factory);
    registerOutputFormatCustomSeparated(factory);
    registerInputFormatForm(factory);

    registerInputFormatORC(factory);
    registerOutputFormatORC(factory);
    registerInputFormatParquet(factory);
    registerOutputFormatParquet(factory);
    registerInputFormatAvro(factory);
    registerOutputFormatAvro(factory);
    registerInputFormatArrow(factory);
    registerOutputFormatArrow(factory);
    registerInputFormatNpy(factory);
    registerOutputFormatNpy(factory);

    registerOutputFormatPretty(factory);
    registerOutputFormatPrettyCompact(factory);
    registerOutputFormatPrettySpace(factory);
    registerOutputFormatVertical(factory);
    registerOutputFormatJSONEachRowWithProgress(factory);
    registerOutputFormatXML(factory);
    registerOutputFormatODBCDriver2(factory);
    registerOutputFormatNull(factory);
    registerOutputFormatMySQLWire(factory);
    registerOutputFormatMarkdown(factory);
    registerOutputFormatPostgreSQLWire(factory);
    registerOutputFormatCapnProto(factory);
    registerOutputFormatPrometheus(factory);
    registerOutputFormatSQLInsert(factory);

    registerInputFormatRegexp(factory);
    registerInputFormatJSONAsString(factory);
    registerInputFormatJSONAsObject(factory);
    registerInputFormatLineAsString(factory);
#if USE_HIVE
    registerInputFormatHiveText(factory);
#endif

    registerInputFormatCapnProto(factory);
    registerInputFormatMySQLDump(factory);

    registerInputFormatParquetMetadata(factory);
    registerInputFormatDWARF(factory);
    registerInputFormatOne(factory);

    registerNonTrivialPrefixAndSuffixCheckerJSONEachRow(factory);
    registerNonTrivialPrefixAndSuffixCheckerJSONAsString(factory);
    registerNonTrivialPrefixAndSuffixCheckerJSONAsObject(factory);

    registerArrowSchemaReader(factory);
    registerParquetSchemaReader(factory);
    registerORCSchemaReader(factory);
    registerTSVSchemaReader(factory);
    registerCSVSchemaReader(factory);
    registerJSONSchemaReader(factory);
    registerJSONCompactEachRowSchemaReader(factory);
    registerJSONEachRowSchemaReader(factory);
    registerJSONObjectEachRowSchemaReader(factory);
    registerJSONAsStringSchemaReader(factory);
    registerJSONAsObjectSchemaReader(factory);
    registerJSONColumnsSchemaReader(factory);
    registerJSONCompactColumnsSchemaReader(factory);
    registerJSONColumnsWithMetadataSchemaReader(factory);
    registerNativeSchemaReader(factory);
    registerRowBinaryWithNamesAndTypesSchemaReader(factory);
    registerAvroSchemaReader(factory);
    registerProtobufSchemaReader(factory);
    registerProtobufListSchemaReader(factory);
    registerLineAsStringSchemaReader(factory);
    registerRawBLOBSchemaReader(factory);
    registerMsgPackSchemaReader(factory);
    registerCapnProtoSchemaReader(factory);
    registerCustomSeparatedSchemaReader(factory);
    registerRegexpSchemaReader(factory);
    registerTSKVSchemaReader(factory);
    registerValuesSchemaReader(factory);
    registerTemplateSchemaReader(factory);
    registerMySQLSchemaReader(factory);
    registerBSONEachRowSchemaReader(factory);
    registerParquetMetadataSchemaReader(factory);
    registerDWARFSchemaReader(factory);
    registerOneSchemaReader(factory);
    registerNpySchemaReader(factory);
    registerFormSchemaReader(factory);
}

}
