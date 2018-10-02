#include <string>

#include <iostream>
#include <fstream>

#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>

#include <Formats/TabSeparatedRowInputStream.h>
#include <Formats/TabSeparatedRowOutputStream.h>
#include <Formats/BlockInputStreamFromRowInputStream.h>
#include <Formats/BlockOutputStreamFromRowOutputStream.h>

#include <DataStreams/copyData.h>


int main(int, char **)
try
{
    using namespace DB;

    Block sample;

    ColumnWithTypeAndName col1;
    col1.name = "col1";
    col1.type = std::make_shared<DataTypeUInt64>();
    col1.column = col1.type->createColumn();
    sample.insert(col1);

    ColumnWithTypeAndName col2;
    col2.name = "col2";
    col2.type = std::make_shared<DataTypeString>();
    col2.column = col2.type->createColumn();
    sample.insert(col2);

    ReadBufferFromFile in_buf("test_in");
    WriteBufferFromFile out_buf("test_out");

    FormatSettings format_settings;

    RowInputStreamPtr row_input = std::make_shared<TabSeparatedRowInputStream>(in_buf, sample, false, false, format_settings);
    BlockInputStreamFromRowInputStream block_input(row_input, sample, DEFAULT_INSERT_BLOCK_SIZE, format_settings);
    RowOutputStreamPtr row_output = std::make_shared<TabSeparatedRowOutputStream>(out_buf, sample, false, false, format_settings);
    BlockOutputStreamFromRowOutputStream block_output(row_output, sample);

    copyData(block_input, block_output);
}
catch (const DB::Exception & e)
{
    std::cerr << e.what() << ", " << e.displayText() << std::endl;
    return 1;
}
