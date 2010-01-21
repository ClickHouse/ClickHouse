#include <iostream>
#include <sstream>
#include <Poco/Types.h>
#include <DB/VarInt.h>
#include <DB/Field.h>

int main(int argc, char ** argv)
{
	DB::Int x = -1000000;
	std::stringstream s;
	DB::writeVarInt(x, s);
	DB::readVarInt(x, s);
	std::cout << x << std::endl;

	return 0;
}
