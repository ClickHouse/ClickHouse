#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <common/StringRef.h>


/** Calculates StringRefHash from stdin. For debugging.
  */

int main(int argc, char ** argv)
{
    using namespace DB;

    ReadBufferFromFileDescriptor in(STDIN_FILENO);
    WriteBufferFromFileDescriptor out(STDOUT_FILENO);

    String s;
    readStringUntilEOF(s, in);
    out << StringRefHash()(s) << '\n';

    return 0;
}
