#include <string>

#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/LimitReadBuffer.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/copyData.h>
#include <IO/WriteHelpers.h>


int main(int argc, char ** argv)
{
    using namespace DB;

    size_t limit = std::stol(argv[1]);

    ReadBufferFromFileDescriptor in(STDIN_FILENO);
    LimitReadBuffer limit_in(in, limit);

    WriteBufferFromFileDescriptor out(STDOUT_FILENO);

    copyData(limit_in, out);
    writeCString("\n--- the rest ---\n", out);
    copyData(in, out);

    return 0;
}
