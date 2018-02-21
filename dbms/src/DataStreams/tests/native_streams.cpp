#include <string>

#include <iostream>
#include <fstream>

#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/CompressedReadBuffer.h>
#include <IO/CompressedWriteBuffer.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeDateTime.h>

#include <DataStreams/NativeBlockInputStream.h>
#include <DataStreams/NativeBlockOutputStream.h>
#include <DataStreams/copyData.h>

#include <Storages/StorageLog.h>

#include <Interpreters/Context.h>
#include <Common/ClickHouseRevision.h>


int main(int argc, char ** argv)
try
{
    using namespace DB;

    NamesAndTypesList names_and_types_list
    {
        {"WatchID", std::make_shared<DataTypeUInt64>()},
        {"JavaEnable", std::make_shared<DataTypeUInt8>()},
        {"Title", std::make_shared<DataTypeString>()},
        {"EventTime", std::make_shared<DataTypeDateTime>()},
        {"CounterID", std::make_shared<DataTypeUInt32>()},
        {"ClientIP", std::make_shared<DataTypeUInt32>()},
        {"RegionID", std::make_shared<DataTypeUInt32>()},
        {"UniqID", std::make_shared<DataTypeUInt64>()},
        {"CounterClass", std::make_shared<DataTypeUInt8>()},
        {"OS", std::make_shared<DataTypeUInt8>()},
        {"UserAgent", std::make_shared<DataTypeUInt8>()},
        {"URL", std::make_shared<DataTypeString>()},
        {"Referer", std::make_shared<DataTypeString>()},
        {"ResolutionWidth", std::make_shared<DataTypeUInt16>()},
        {"ResolutionHeight", std::make_shared<DataTypeUInt16>()},
        {"ResolutionDepth", std::make_shared<DataTypeUInt8>()},
        {"FlashMajor", std::make_shared<DataTypeUInt8>()},
        {"FlashMinor", std::make_shared<DataTypeUInt8>()},
        {"FlashMinor2", std::make_shared<DataTypeString>()},
        {"NetMajor", std::make_shared<DataTypeUInt8>()},
        {"NetMinor", std::make_shared<DataTypeUInt8>()},
        {"UserAgentMajor", std::make_shared<DataTypeUInt16>()},
        {"UserAgentMinor", std::make_shared<DataTypeFixedString>(2)},
        {"CookieEnable", std::make_shared<DataTypeUInt8>()},
        {"JavascriptEnable", std::make_shared<DataTypeUInt8>()},
        {"IsMobile", std::make_shared<DataTypeUInt8>()},
        {"MobilePhone", std::make_shared<DataTypeUInt8>()},
        {"MobilePhoneModel", std::make_shared<DataTypeString>()},
        {"Params", std::make_shared<DataTypeString>()},
        {"IPNetworkID", std::make_shared<DataTypeUInt32>()},
        {"TraficSourceID", std::make_shared<DataTypeInt8>()},
        {"SearchEngineID", std::make_shared<DataTypeUInt16>()},
        {"SearchPhrase", std::make_shared<DataTypeString>()},
        {"AdvEngineID", std::make_shared<DataTypeUInt8>()},
        {"IsArtifical", std::make_shared<DataTypeUInt8>()},
        {"WindowClientWidth", std::make_shared<DataTypeUInt16>()},
        {"WindowClientHeight", std::make_shared<DataTypeUInt16>()},
        {"ClientTimeZone", std::make_shared<DataTypeInt16>()},
        {"ClientEventTime", std::make_shared<DataTypeDateTime>()},
        {"SilverlightVersion1", std::make_shared<DataTypeUInt8>()},
        {"SilverlightVersion2", std::make_shared<DataTypeUInt8>()},
        {"SilverlightVersion3", std::make_shared<DataTypeUInt32>()},
        {"SilverlightVersion4", std::make_shared<DataTypeUInt16>()},
        {"PageCharset", std::make_shared<DataTypeString>()},
        {"CodeVersion", std::make_shared<DataTypeUInt32>()},
        {"IsLink", std::make_shared<DataTypeUInt8>()},
        {"IsDownload", std::make_shared<DataTypeUInt8>()},
        {"IsNotBounce", std::make_shared<DataTypeUInt8>()},
        {"FUniqID", std::make_shared<DataTypeUInt64>()},
        {"OriginalURL", std::make_shared<DataTypeString>()},
        {"HID", std::make_shared<DataTypeUInt32>()},
        {"IsOldCounter", std::make_shared<DataTypeUInt8>()},
        {"IsEvent", std::make_shared<DataTypeUInt8>()},
        {"IsParameter", std::make_shared<DataTypeUInt8>()},
        {"DontCountHits", std::make_shared<DataTypeUInt8>()},
        {"WithHash", std::make_shared<DataTypeUInt8>()},
    };

    Names column_names;

    for (const auto & name_type : names_and_types_list)
        column_names.push_back(name_type.name);

    /// create an object of an existing hit log table

    StoragePtr table = StorageLog::create("./", "HitLog", names_and_types_list,
        NamesAndTypesList{}, NamesAndTypesList{}, ColumnDefaults{}, DEFAULT_MAX_COMPRESS_BLOCK_SIZE);
    table->startup();

    /// read from it
    if (argc == 2 && 0 == strcmp(argv[1], "read"))
    {
        QueryProcessingStage::Enum stage;
        BlockInputStreamPtr in = table->read(column_names, {}, Context::createGlobal(), stage, 8192, 1)[0];
        WriteBufferFromFileDescriptor out1(STDOUT_FILENO);
        CompressedWriteBuffer out2(out1);
        NativeBlockOutputStream out3(out2, ClickHouseRevision::get(), in->getHeader());
        copyData(*in, out3);
    }

    /// read the data from the native file and simultaneously write to the table
    if (argc == 2 && 0 == strcmp(argv[1], "write"))
    {
        ReadBufferFromFileDescriptor in1(STDIN_FILENO);
        CompressedReadBuffer in2(in1);
        NativeBlockInputStream in3(in2, ClickHouseRevision::get());
        BlockOutputStreamPtr out = table->write({}, {});
        copyData(in3, *out);
    }

    return 0;
}
catch (const DB::Exception & e)
{
    std::cerr << e.what() << ", " << e.displayText() << std::endl;
    throw;
}
