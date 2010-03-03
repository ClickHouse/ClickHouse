#include <iostream>
#include <sstream>

#include <DB/Common/EscapingOutputStream.h>
#include <DB/Common/UnescapingInputStream.h>


int main(int argc, char ** argv)
{
	std::string s1("abc'd\"e\\f\\"), s2;
	std::stringstream stream;

	DB::EscapingOutputStream o(stream);
	DB::UnescapingInputStream i(stream, '"');
	
	std::cout << s1 << std::endl;
	o << s1;
	stream << "\"xxx";
	std::cout << stream.str() << std::endl;
	std::getline(i, s2);
	std::cout << s2 << std::endl;

	return 0;
}
