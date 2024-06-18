#include <string>

#include <iostream>
#include <IO/VarInt.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Poco/HexBinaryEncoder.h>


int main(int argc, char ** argv)
{
    if (argc != 2)
    {
        std::cerr << "Usage: " << std::endl
            << argv[0] << " unsigned_number" << std::endl;
        return 1;
    }

    UInt64 x = DB::parse<UInt64>(argv[1]);

    std::cout << std::hex << std::showbase << "Input: " << x << std::endl;

    Poco::HexBinaryEncoder hex(std::cout);
    std::cout << "writeVarUInt(std::ostream): 0x";
    DB::writeVarUInt(x, hex);
    std::cout << std::endl;

    std::string s;

    {
        DB::WriteBufferFromString wb(s);
        DB::writeVarUInt(x, wb);
        wb.next();
    }

    std::cout << "writeVarUInt(WriteBuffer): 0x";
    hex << s;
    std::cout << std::endl;

    s.clear();
    s.resize(9);

    s.resize(DB::writeVarUInt(x, s.data()) - s.data());

    std::cout << "writeVarUInt(char *): 0x";
    hex << s;
    std::cout << std::endl;

    UInt64 y = 0;

    DB::ReadBufferFromString rb(s);
    DB::readVarUInt(y, rb);

    std::cerr << "Input: " << x << ", readVarUInt(writeVarUInt()): " << y << std::endl;

    return 0;
}
