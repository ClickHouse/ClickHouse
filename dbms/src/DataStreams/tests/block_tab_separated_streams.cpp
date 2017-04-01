#include <map>
#include <list>
#include <iostream>

#include <IO/ReadBufferFromIStream.h>
#include <IO/WriteBufferFromOStream.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeDateTime.h>

#include <DataStreams/TabSeparatedRowInputStream.h>
#include <DataStreams/TabSeparatedBlockOutputStream.h>
#include <DataStreams/BlockInputStreamFromRowInputStream.h>
#include <DataStreams/copyData.h>

#include <Storages/StorageLog.h>


using namespace DB;


int main(int argc, char ** argv)
try
{
    NamesAndTypesList names_and_types_list
    {
        {"WatchID",                std::make_shared<DataTypeUInt64>()},
        {"JavaEnable",            std::make_shared<DataTypeUInt8>()},
        {"Title",                std::make_shared<DataTypeString>()},
        {"EventTime",            std::make_shared<DataTypeDateTime>()},
        {"CounterID",            std::make_shared<DataTypeUInt32>()},
        {"ClientIP",            std::make_shared<DataTypeUInt32>()},
        {"RegionID",            std::make_shared<DataTypeUInt32>()},
        {"UniqID",                std::make_shared<DataTypeUInt64>()},
        {"CounterClass",        std::make_shared<DataTypeUInt8>()},
        {"OS",                    std::make_shared<DataTypeUInt8>()},
        {"UserAgent",            std::make_shared<DataTypeUInt8>()},
        {"URL",                    std::make_shared<DataTypeString>()},
        {"Referer",                std::make_shared<DataTypeString>()},
        {"ResolutionWidth",        std::make_shared<DataTypeUInt16>()},
        {"ResolutionHeight",    std::make_shared<DataTypeUInt16>()},
        {"ResolutionDepth",        std::make_shared<DataTypeUInt8>()},
        {"FlashMajor",            std::make_shared<DataTypeUInt8>()},
        {"FlashMinor",            std::make_shared<DataTypeUInt8>()},
        {"FlashMinor2",            std::make_shared<DataTypeString>()},
        {"NetMajor",            std::make_shared<DataTypeUInt8>()},
        {"NetMinor",            std::make_shared<DataTypeUInt8>()},
        {"UserAgentMajor",        std::make_shared<DataTypeUInt16>()},
        {"UserAgentMinor",        std::make_shared<DataTypeFixedString>(2)},
        {"CookieEnable",        std::make_shared<DataTypeUInt8>()},
        {"JavascriptEnable",    std::make_shared<DataTypeUInt8>()},
        {"IsMobile",            std::make_shared<DataTypeUInt8>()},
        {"MobilePhone",            std::make_shared<DataTypeUInt8>()},
        {"MobilePhoneModel",    std::make_shared<DataTypeString>()},
        {"Params",                std::make_shared<DataTypeString>()},
        {"IPNetworkID",            std::make_shared<DataTypeUInt32>()},
        {"TraficSourceID",        std::make_shared<DataTypeInt8>()},
        {"SearchEngineID",        std::make_shared<DataTypeUInt16>()},
        {"SearchPhrase",        std::make_shared<DataTypeString>()},
        {"AdvEngineID",            std::make_shared<DataTypeUInt8>()},
        {"IsArtifical",            std::make_shared<DataTypeUInt8>()},
        {"WindowClientWidth",    std::make_shared<DataTypeUInt16>()},
        {"WindowClientHeight",    std::make_shared<DataTypeUInt16>()},
        {"ClientTimeZone",        std::make_shared<DataTypeInt16>()},
        {"ClientEventTime",        std::make_shared<DataTypeDateTime>()},
        {"SilverlightVersion1",    std::make_shared<DataTypeUInt8>()},
        {"SilverlightVersion2",    std::make_shared<DataTypeUInt8>()},
        {"SilverlightVersion3",    std::make_shared<DataTypeUInt32>()},
        {"SilverlightVersion4",    std::make_shared<DataTypeUInt16>()},
        {"PageCharset",            std::make_shared<DataTypeString>()},
        {"CodeVersion",            std::make_shared<DataTypeUInt32>()},
        {"IsLink",                std::make_shared<DataTypeUInt8>()},
        {"IsDownload",            std::make_shared<DataTypeUInt8>()},
        {"IsNotBounce",            std::make_shared<DataTypeUInt8>()},
        {"FUniqID",                std::make_shared<DataTypeUInt64>()},
        {"OriginalURL",            std::make_shared<DataTypeString>()},
        {"HID",                    std::make_shared<DataTypeUInt32>()},
        {"IsOldCounter",        std::make_shared<DataTypeUInt8>()},
        {"IsEvent",                std::make_shared<DataTypeUInt8>()},
        {"IsParameter",            std::make_shared<DataTypeUInt8>()},
        {"DontCountHits",        std::make_shared<DataTypeUInt8>()},
        {"WithHash",            std::make_shared<DataTypeUInt8>()},
    };

    /// we create a description of how to read data from the tab separated dump

    Block sample;
    for (const auto & name_type : names_and_types_list)
    {
        ColumnWithTypeAndName elem;
        elem.name = name_type.name;
        elem.type = name_type.type;
        elem.column = elem.type->createColumn();
        sample.insert(std::move(elem));
    }

    /// read the data from row tsv file and simultaneously write to the block tsv file
    {
        ReadBufferFromIStream in_buf(std::cin);
        WriteBufferFromOStream out_buf(std::cout);

        RowInputStreamPtr row_in = std::make_shared<TabSeparatedRowInputStream>(in_buf, sample);
        BlockInputStreamFromRowInputStream in(row_in, sample, DEFAULT_INSERT_BLOCK_SIZE, 0, 0);
        TabSeparatedBlockOutputStream out(out_buf);
        copyData(in, out);
    }

    return 0;
}
catch (const Exception & e)
{
    std::cerr << e.what() << ", " << e.displayText() << std::endl;
    return 1;
}
