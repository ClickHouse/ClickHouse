#include <limits>
#include <type_traits>
#include <Core/Types.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/Operators.h>


using namespace DB;


void NO_INLINE write(ReadBuffer & in, WriteBuffer & out, size_t size)
{
    for (size_t i = 0; i < size; ++i)
    {
        writeIntText(i, out);

        int tmp;
        readIntText(tmp, in);

        out << "parsed text: \n";

        if (in.eof() || *in.position() != '\t')
            return;
    }
}


int main(int, char **)
{
    std::string str = "0\t1\t2\t3\t4\t5\t6\t7\t8\t9\t10\ta\n";
    ReadBufferFromString in(str);
    WriteBufferFromFileDescriptor out(STDOUT_FILENO);

    write(in, out, 12);
    return 0;
}
