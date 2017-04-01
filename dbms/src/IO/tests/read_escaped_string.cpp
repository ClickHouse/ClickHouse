#include <string>

#include <iostream>

#include <DB/Core/Types.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/ReadBufferFromMemory.h>
#include <DB/IO/ConcatReadBuffer.h>


using namespace DB;

int main(int argc, char ** argv)
try
{
    std::string s1 = "abc\\x\n";
    std::string s2 = "\tdef";

    ReadBufferFromMemory rb1(s1.data(), 3);
    ReadBufferFromMemory rb2(s2.data(), s2.size());

    ConcatReadBuffer rb3(rb1, rb2);

    std::string read_s1;
    std::string read_s2;

    readEscapedString(read_s1, rb3);
    assertChar('\t', rb3);
    readEscapedString(read_s2, rb3);

    std::cerr << read_s1 << ", " << read_s2 << std::endl;
    std::cerr << ((read_s1 == "abc" && read_s2 == "def") ? "Ok." : "Fail.") << std::endl;

    return 0;
}
catch (const Exception & e)
{
    std::cerr << e.what() << ", " << e.displayText() << std::endl;
    return 1;
}
