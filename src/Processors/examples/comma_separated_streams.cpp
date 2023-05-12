#include <string>
#include <iostream>
#include <fstream>

#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <Processors/Formats/Impl/HiveTextRowInputFormat.h>
#include <Processors/Formats/Impl/CSVRowOutputFormat.h>
#include <Formats/FormatFactory.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Processors/Executors/PullingPipelineExecutor.h>

using namespace DB;

int main()
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
    format_settings.with_names_use_header = true;
    format_settings.skip_unknown_fields = true;
    format_settings.csv.delimiter = '\x01';
    format_settings.hive_text.input_field_names =
    {
        "d",
        "e",
        "f",
        "a",
        "b",
        "c",
        "g",
        "h",
        "i",
        "j",
    };

    RowInputFormatParams in_params{DEFAULT_INSERT_BLOCK_SIZE};
    InputFormatPtr input_format = std::make_shared<HiveTextRowInputFormat>(sample, in_buf, in_params, format_settings);
    auto pipeline = QueryPipeline(std::move(input_format));
    auto reader = std::make_unique<PullingPipelineExecutor>(pipeline);

    RowOutputFormatParams out_params;
    OutputFormatPtr output_format = std::make_shared<CSVRowOutputFormat>(out_buf, sample, true, true, out_params, format_settings);
    Block res;
    while (reader->pull(res))
    {
        output_format->write(res);
    }
    return 0;
}
catch (...)
{
    std::cerr << getCurrentExceptionMessage(true) << '\n';
    return 1;
}
