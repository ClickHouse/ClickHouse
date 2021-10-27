#include <string>

#include <iostream>
#include <fstream>

#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>

#include <Processors/Formats/Impl/CSVRowInputFormat.h>

#include <DataStreams/copyData.h>
#include <Processors/Formats/OutputStreamToOutputFormat.h>
#include <Processors/Formats/Impl/CSVRowOutputFormat.h>
#include <Processors/Formats/InputStreamFromInputFormat.h>


using namespace DB;

int main(int, char **)
try
{
    Block sample;
    {
        // a
        ColumnWithTypeAndName col;
        col.name = "a";
        col.type = std::make_shared<DataTypeInt64>();
        sample.insert(std::move(col));
    }
    {
        // b
        ColumnWithTypeAndName col;
        col.name = "b";
        col.type = std::make_shared<DataTypeFloat64>();
        sample.insert(std::move(col));
    }
    {
        // c
        ColumnWithTypeAndName col;
        col.name = "c";
        col.type = std::make_shared<DataTypeInt64>();
        sample.insert(std::move(col));
    }
    {
        // d
        ColumnWithTypeAndName col;
        col.name = "d";
        col.type = std::make_shared<DataTypeString>();
        sample.insert(std::move(col));
    }
    {
        // e
        ColumnWithTypeAndName col;
        col.name = "e";
        col.type = std::make_shared<DataTypeString>();
        sample.insert(std::move(col));
    }
    {
        // f
        ColumnWithTypeAndName col;
        col.name = "f";
        col.type = std::make_shared<DataTypeString>();
        sample.insert(std::move(col));
    }
    {
        // g
        ColumnWithTypeAndName col;
        col.name = "g";
        col.type = std::make_shared<DataTypeInt64>();
        sample.insert(std::move(col));
    }
    {
        // h
        ColumnWithTypeAndName col;
        col.name = "h";
        col.type = std::make_shared<DataTypeInt64>();
        sample.insert(std::move(col));
    }


    ReadBufferFromFile in_buf("test_in");
    WriteBufferFromFile out_buf("test_out");

    FormatSettings format_settings;
    format_settings.csv.input_field_names =
    {
        "d",
        "e",
        "f",
        "a",
        "b",
        "c",
        "g",
        "h",
    };
    format_settings.csv.delimiter = '\x01';
    format_settings.with_names_use_header = true;

    RowInputFormatParams in_params{DEFAULT_INSERT_BLOCK_SIZE};
    InputFormatPtr input_format = std::make_shared<CSVRowInputFormat>(sample, in_buf, in_params, true, format_settings);
    BlockInputStreamPtr block_input = std::make_shared<InputStreamFromInputFormat>(std::move(input_format));

    RowOutputFormatParams out_params;
    BlockOutputStreamPtr block_output = std::make_shared<OutputStreamToOutputFormat>(
        std::make_shared<CSVRowOutputFormat>(out_buf, sample, false, out_params, format_settings));
    copyData(*block_input, *block_output);
    return 0;
}
catch (...)
{
    std::cerr << getCurrentExceptionMessage(true) << '\n';
    return 1;
}
