#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/LimitReadBuffer.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/copyData.h>
#include <IO/WriteHelpers.h>

#include <iostream>
#include <string>


int main(int argc, char ** argv)
{
    using namespace DB;

    if (argc < 2)
    {
        std::cerr << "Usage: program limit < in > out\n";
        return 1;
    }

    UInt64 limit = std::stol(argv[1]);

    ReadBufferFromFileDescriptor in(STDIN_FILENO);
    WriteBufferFromFileDescriptor out(STDOUT_FILENO);

    writeCString("--- first ---\n", out);
    {
        LimitReadBuffer limit_in(in, {.read_no_more=limit});
        copyData(limit_in, out);
    }

    writeCString("\n--- second ---\n", out);
    {
        LimitReadBuffer limit_in(in,{.read_no_more=limit});
        copyData(limit_in, out);
    }

    writeCString("\n--- the rest ---\n", out);
    copyData(in, out);

    out.finalize();

    return 0;
}
