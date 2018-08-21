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

        IDataType::SerializeBinaryBulkSettings settings;
        IDataType::SerializeBinaryBulkStatePtr state;
        settings.getter = [&](const IDataType::SubstreamPath &){ return &out_buf; };

        stopwatch.restart();
        data_type.serializeBinaryBulkStatePrefix(settings, state);
        data_type.serializeBinaryBulkWithMultipleStreams(*column, 0, 0, settings, state);
        data_type.serializeBinaryBulkStateSuffix(settings, state);
        stopwatch.stop();

        std::cout << "Writing, elapsed: " << stopwatch.elapsedSeconds() << std::endl;
    }

    {
        auto column = ColumnString::create();

        ReadBufferFromFile in_buf("test");

        IDataType::DeserializeBinaryBulkSettings settings;
        IDataType::DeserializeBinaryBulkStatePtr state;
        settings.getter = [&](const IDataType::SubstreamPath &){ return &in_buf; };

        stopwatch.restart();
        data_type.deserializeBinaryBulkStatePrefix(settings, state);
        data_type.deserializeBinaryBulkWithMultipleStreams(*column, n, settings, state);
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
