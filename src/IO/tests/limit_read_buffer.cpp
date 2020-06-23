#include <string>

#include "src/IO/ReadBufferFromFileDescriptor.h"
#include "src/IO/LimitReadBuffer.h"
#include "src/IO/WriteBufferFromFileDescriptor.h"
#include "src/IO/copyData.h"
#include "src/IO/WriteHelpers.h"


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
        LimitReadBuffer limit_in(in, limit, false);
        copyData(limit_in, out);
    }

    writeCString("\n--- second ---\n", out);
    {
        LimitReadBuffer limit_in(in, limit, false);
        copyData(limit_in, out);
    }

    writeCString("\n--- the rest ---\n", out);
    copyData(in, out);

    return 0;
}
