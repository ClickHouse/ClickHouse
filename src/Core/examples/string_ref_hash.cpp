#include <base/StringViewHash.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/Operators.h>


/** Calculates StringViewHash from stdin. For debugging.
  */

int main(int, char **)
{
    using namespace DB;

    ReadBufferFromFileDescriptor in(STDIN_FILENO);
    WriteBufferFromFileDescriptor out(STDOUT_FILENO);

    String s;
    readStringUntilEOF(s, in);
    out << StringViewHash()(s) << '\n';
    out.finalize();

    return 0;
}
