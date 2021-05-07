#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/Operators.h>
#include <common/StringRef.h>


/** Calculates StringRefHash from stdin. For debugging.
  */

int main(int, char **)
{
    using namespace DB;

    ReadBufferFromFileDescriptor in(STDIN_FILENO);
    WriteBufferFromFileDescriptor out(STDOUT_FILENO);

    String s;
    readStringUntilEOF(s, in);
    out << StringRefHash()(s) << '\n';

    return 0;
}
