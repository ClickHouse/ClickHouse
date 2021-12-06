#include <vector>
#include <string>
#include <iomanip>

#include <Common/SipHash.h>

#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/Stopwatch.h>

using namespace DB;

int main(int argc, char **argv)
{
    (void)(argc);
    (void)(argv);

    std::string buffer;

    ReadBufferFromFileDescriptor read_buffer(0);
    WriteBufferFromFileDescriptor write_buffer(1);
    size_t rows = 0;
    char dummy;

    while (!read_buffer.eof())
    {
        readIntText(rows, read_buffer);
        readChar(dummy, read_buffer);

        for (size_t i = 0; i < rows; ++i)
        {
            readString(buffer, read_buffer);
            readChar(dummy, read_buffer);

            writeString("Key ", write_buffer);
            writeString(buffer, write_buffer);
            writeChar('\n', write_buffer);
        }

        write_buffer.next();
    }

    return 0;
}
