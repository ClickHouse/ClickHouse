#include <string>

#include <iostream>
#include <DB/IO/VarInt.h>
#include <Poco/NumberParser.h>
#include <Poco/HexBinaryEncoder.h>


int main(int argc, char ** argv)
{
	if (argc != 2)
	{
		std::cerr << "Usage: " << std::endl
			<< argv[0] << " unsigned_number" << std::endl;
		return 1;
	}
	
	DB::UInt64 x = Poco::NumberParser::parseUnsigned64(argv[1]);
	Poco::HexBinaryEncoder hex(std::cout);
	DB::writeVarUInt(x, hex);
	std::cout << std::endl;
	
	return 0;
}
