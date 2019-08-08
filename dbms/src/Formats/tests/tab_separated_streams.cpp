#include <string>

#include <iostream>
#include <fstream>

#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>

#include <Formats/TabSeparatedRowInputStream.h>
#include <Formats/BlockInputStreamFromRowInputStream.h>
#include <Formats/BlockOutputStreamFromRowOutputStream.h>

#include <DataStreams/copyData.h>
#include <Processors/Formats/OutputStreamToOutputFormat.h>
#include <Processors/Formats/Impl/TabSeparatedRowOutputFormat.h>


using namespace DB;

int main(int, char **)
try
{
    Block sample;
    {
        ColumnWithTypeAndName col;
        col.type = std::make_shared<DataTypeUInt64>();
        sample.insert(std::move(col));
    }
    {
        ColumnWithTypeAndName col;
        col.type = std::make_shared<DataTypeString>();
        sample.insert(std::move(col));
    }

    ReadBufferFromFile in_buf("test_in");
    WriteBufferFromFile out_buf("test_out");

    FormatSettings format_settings;

    RowInputStreamPtr row_input = std::make_shared<TabSeparatedRowInputStream>(in_buf, sample, false, false, format_settings);
    BlockInputStreamFromRowInputStream block_input(row_input, sample, DEFAULT_INSERT_BLOCK_SIZE, 0, []{}, format_settings);

    BlockOutputStreamPtr block_output = std::make_shared<OutputStreamToOutputFormat>(
            std::make_shared<TabSeparatedRowOutputFormat>(out_buf, sample, false, false, format_settings));

    copyData(block_input, *block_output);
    return 0;
}
catch (...)
{
    std::cerr << getCurrentExceptionMessage(true) << '\n';
    return 1;
}
