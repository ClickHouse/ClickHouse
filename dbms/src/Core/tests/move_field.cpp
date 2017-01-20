#include <iostream>
#include <DB/Core/Field.h>


int main(int argc, char ** argv)
{
	using namespace DB;

	Field f;

	f = Field{String{"Hello, world"}};
	std::cerr << f.get<String>() << "\n";
	f = Field{String{"Hello, world!"}};
	std::cerr << f.get<String>() << "\n";

	return 0;
}
