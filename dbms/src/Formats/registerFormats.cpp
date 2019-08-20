#include <Formats/FormatFactory.h>
#include <Formats/registerFormats.h>

namespace DB
{

void registerInputFormatCSV(FormatFactory & factory);
void registerInputFormatNative(FormatFactory & factory);
void registerInputFormatProcessorCSV(FormatFactory & factory);
void registerInputFormatProcessorCapnProto(FormatFactory & factory);
void registerInputFormatProcessorJSONEachRow(FormatFactory & factory);
void registerInputFormatProcessorNative(FormatFactory & factory);
void registerInputFormatProcessorParquet(FormatFactory & factory);
void registerInputFormatProcessorProtobuf(FormatFactory & factory);
void registerInputFormatProcessorRowBinary(FormatFactory & factory);
void registerInputFormatProcessorTSKV(FormatFactory & factory);
void registerInputFormatProcessorTabSeparated(FormatFactory & factory);
void registerInputFormatProcessorValues(FormatFactory & factory);
void registerInputFormatTabSeparated(FormatFactory & factory);
void registerOutputFormatNative(FormatFactory & factory);
void registerOutputFormatNull(FormatFactory & factory);
void registerOutputFormatProcessorCSV(FormatFactory & factory);
void registerOutputFormatProcessorJSON(FormatFactory & factory);
void registerOutputFormatProcessorJSONCompact(FormatFactory & factory);
void registerOutputFormatProcessorJSONEachRow(FormatFactory & factory);
void registerOutputFormatProcessorMySQLWrite(FormatFactory & factory);
void registerOutputFormatProcessorNative(FormatFactory & factory);
void registerOutputFormatProcessorNull(FormatFactory & factory);
void registerOutputFormatProcessorODBCDriver(FormatFactory & factory);
void registerOutputFormatProcessorODBCDriver2(FormatFactory & factory);
void registerOutputFormatProcessorParquet(FormatFactory & factory);
void registerOutputFormatProcessorPretty(FormatFactory & factory);
void registerOutputFormatProcessorPrettyCompact(FormatFactory & factory);
void registerOutputFormatProcessorPrettySpace(FormatFactory & factory);
void registerOutputFormatProcessorProtobuf(FormatFactory & factory);
void registerOutputFormatProcessorRowBinary(FormatFactory & factory);
void registerOutputFormatProcessorTSKV(FormatFactory & factory);
void registerOutputFormatProcessorTabSeparated(FormatFactory & factory);
void registerOutputFormatProcessorValues(FormatFactory & factory);
void registerOutputFormatProcessorVertical(FormatFactory & factory);
void registerOutputFormatProcessorXML(FormatFactory & factory);

void registerFormats()
{
    auto & factory = FormatFactory::instance();

    registerInputFormatNative(factory);
    registerOutputFormatNative(factory);
    registerInputFormatTabSeparated(factory);
    registerInputFormatCSV(factory);

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
    registerInputFormatProcessorProtobuf(factory);
    registerOutputFormatProcessorProtobuf(factory);
    registerInputFormatProcessorCapnProto(factory);
    registerInputFormatProcessorParquet(factory);
    registerOutputFormatProcessorParquet(factory);

    registerOutputFormatNull(factory);

    registerOutputFormatProcessorPretty(factory);
    registerOutputFormatProcessorPrettyCompact(factory);
    registerOutputFormatProcessorPrettySpace(factory);
    registerOutputFormatProcessorVertical(factory);
    registerOutputFormatProcessorJSON(factory);
    registerOutputFormatProcessorJSONCompact(factory);
    registerOutputFormatProcessorXML(factory);
    registerOutputFormatProcessorODBCDriver(factory);
    registerOutputFormatProcessorODBCDriver2(factory);
    registerOutputFormatProcessorNull(factory);
    registerOutputFormatProcessorMySQLWrite(factory);
}

}
