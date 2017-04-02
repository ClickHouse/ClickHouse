#include <string>

#include <iostream>
#include <fstream>

#include <Common/Stopwatch.h>

#include <IO/ReadBufferFromIStream.h>
#include <IO/WriteBufferFromOStream.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>


int main(int argc, char ** argv)
try
{
    using namespace DB;

    Stopwatch stopwatch;
    size_t n = 50000000;
    const char * s = "";
    size_t size = strlen(s) + 1;
    DataTypeString data_type;

    {
        std::shared_ptr<ColumnString> column = std::make_shared<ColumnString>();
        ColumnString::Chars_t & data = column->getChars();
        ColumnString::Offsets_t & offsets = column->getOffsets();

        data.resize(n * size);
        offsets.resize(n);
        for (size_t i = 0; i < n; ++i)
        {
            memcpy(&data[i * size], s, size);
            offsets[i] = (i + 1) * size;
        }

        std::ofstream ostr("test");
        WriteBufferFromOStream out_buf(ostr);

        stopwatch.restart();
        data_type.serializeBinaryBulk(*column, out_buf, 0, 0);
        stopwatch.stop();

        std::cout << "Writing, elapsed: " << stopwatch.elapsedSeconds() << std::endl;
    }

    {
        std::shared_ptr<ColumnString> column = std::make_shared<ColumnString>();

        std::ifstream istr("test");
        ReadBufferFromIStream in_buf(istr);

        stopwatch.restart();
        data_type.deserializeBinaryBulk(*column, in_buf, n, 0);
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
