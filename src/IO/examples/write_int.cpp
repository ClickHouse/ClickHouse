#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromFileDescriptor.h>

using namespace DB;

static void NO_INLINE write(WriteBuffer & out, size_t size)
{
    for (size_t i = 0; i < size; ++i)
    {
        writeIntText(i, out);
        writeChar(' ', out);
    }
}


int main(int, char **)
{
    WriteBufferFromFileDescriptor out(STDOUT_FILENO);
    write(out, 80);
    out.finalize();

    return 0;
}
