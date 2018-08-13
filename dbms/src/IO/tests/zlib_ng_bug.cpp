#include <Poco/FileStream.h>
#include <Poco/NullStream.h>
#include <Poco/StreamCopier.h>
#include <Poco/DeflatingStream.h>

/** This script reproduces the bug in zlib-ng library.
  * Put the following content to "data.bin" file:
abcdefghijklmn!@Aab#AAabcdefghijklmn$%
xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
  * There are two lines. First line make sense. Second line contains padding to make file size large enough.
  * Compile with 
  *  cmake -D SANITIZE=address
  * and run:

./zlib_ng_bug data2.bin 
=================================================================
==204952==ERROR: AddressSanitizer: heap-buffer-overflow on address 0x6310000147ff at pc 0x000000596d7a bp 0x7ffd139edd50 sp 0x7ffd139edd48
READ of size 1 at 0x6310000147ff thread T0
  */

int main(int argc, char ** argv)
{
    using namespace Poco;

    std::string filename(argc >= 2 ? argv[1] : "data.bin");
    FileInputStream istr(filename);
    NullOutputStream ostr;
    DeflatingOutputStream deflater(ostr, DeflatingStreamBuf::STREAM_GZIP);
    StreamCopier::copyStream(istr, deflater);

    return 0;
}
