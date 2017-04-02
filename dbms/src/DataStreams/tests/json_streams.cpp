#include <map>
#include <list>
#include <iostream>
#include <fstream>

#include <IO/ReadBufferFromIStream.h>
#include <IO/WriteBufferFromOStream.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeDateTime.h>

#include <DataStreams/TabSeparatedRowInputStream.h>
#include <DataStreams/TabSeparatedBlockOutputStream.h>
#include <DataStreams/JSONRowOutputStream.h>
#include <DataStreams/JSONCompactRowOutputStream.h>
#include <DataStreams/BlockInputStreamFromRowInputStream.h>
#include <DataStreams/BlockOutputStreamFromRowOutputStream.h>
#include <DataStreams/copyData.h>

#include <Storages/StorageLog.h>


using namespace DB;


int main(int argc, char ** argv)
try
{
    NamesAndTypesList names_and_types_list
    {
        {"WatchID",                std::make_shared<DataTypeUInt64>()},
        {"ClientIP",            std::make_shared<DataTypeUInt32>()},
        {"Referer",                std::make_shared<DataTypeString>()},
        {"URL",                    std::make_shared<DataTypeString>()},
        {"IsLink",                std::make_shared<DataTypeUInt8>()},
        {"OriginalUserAgent",    std::make_shared<DataTypeString>()},
        {"EventTime",            std::make_shared<DataTypeDateTime>()},
    };

    Block sample;
    for (const auto & name_type : names_and_types_list)
    {
        ColumnWithTypeAndName elem;
        elem.name = name_type.name;
        elem.type = name_type.type;
        elem.column = elem.type->createColumn();
        sample.insert(std::move(elem));
    }

    {
        std::ifstream istr("json_test.in");
        std::ofstream ostr("json_test.out");

        ReadBufferFromIStream in_buf(istr);
        WriteBufferFromOStream out_buf(ostr);

        BlockInputStreamFromRowInputStream in(std::make_shared<TabSeparatedRowInputStream>(in_buf, sample, true, true),
            sample, DEFAULT_INSERT_BLOCK_SIZE, 0, 0);
        BlockOutputStreamFromRowOutputStream out(std::make_shared<JSONRowOutputStream>(out_buf, sample, false, true));
        copyData(in, out);
    }

    return 0;
}
catch (const Exception & e)
{
    std::cerr << e.what() << ", " << e.displayText() << std::endl;
    throw;
}
