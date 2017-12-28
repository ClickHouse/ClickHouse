#include <string>

#include <iostream>
#include <fstream>

#include <Common/Stopwatch.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>


int main(int, char **)
try
{
    using namespace DB;

    Stopwatch stopwatch;
    size_t n = 50000000;
    const char * s = "";
    size_t size = strlen(s) + 1;
    DataTypeString data_type;

    {
        auto column = ColumnString::create();
        ColumnString::Chars_t & data = column->getChars();
        ColumnString::Offsets & offsets = column->getOffsets();

        data.resize(n * size);
        offsets.resize(n);
        for (size_t i = 0; i < n; ++i)
        {
            memcpy(&data[i * size], s, size);
            offsets[i] = (i + 1) * size;
        }

        WriteBufferFromFile out_buf("test");

        stopwatch.restart();
        data_type.serializeBinaryBulkWithMultipleStreams(*column, [&](const IDataType::SubstreamPath &){ return &out_buf; }, 0, 0, true, {});
        stopwatch.stop();

        std::cout << "Writing, elapsed: " << stopwatch.elapsedSeconds() << std::endl;
    }

    {
        auto column = ColumnString::create();

        ReadBufferFromFile in_buf("test");

        stopwatch.restart();
        data_type.deserializeBinaryBulkWithMultipleStreams(*column, [&](const IDataType::SubstreamPath &){ return &in_buf; }, n, 0, true, {});
        stopwatch.stop();

        std::cout << "Reading, elapsed: " << stopwatch.elapsedSeconds() << std::endl;

        std::cout << std::endl
            << get<const String &>((*column)[0]) << std::endl
            << get<const String &>((*column)[n - 1]) << std::endl;
    }

    return 0;
}
catch (const DB::Exception & e)
{
    std::cerr << e.what() << ", " << e.displayText() << std::endl;
    return 1;
}
