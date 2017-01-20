#include <iostream>
#include <DB/Core/Field.h>


int main(int argc, char ** argv)
{
	using namespace DB;

	Field f;

	f = std::string("Hello, world");
	std::cerr << f.get<std::string>() << "\n";
	f = std::string("Hello, world!");
	std::cerr << f.get<std::string>() << "\n";

	return 0;
}
