#include <string>

#include <iostream>

#include <DB/Core/Types.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/ConcatReadBuffer.h>


int main(int argc, char ** argv)
{
	try
	{
		std::string s1 = "abc\\x\n";
		std::string s2 = "\tdef";

		DB::ReadBuffer rb1(const_cast<char *>(s1.data()), 3, 0);
		DB::ReadBuffer rb2(const_cast<char *>(s2.data()), s2.size(), 0);

		DB::ConcatReadBuffer rb3(rb1, rb2);

		std::string read_s1;
		std::string read_s2;

		DB::readEscapedString(read_s1, rb3);
		DB::assertChar('\t', rb3);
		DB::readEscapedString(read_s2, rb3);

		std::cerr << read_s1 << ", " << read_s2 << std::endl;
		std::cerr << ((read_s1 == "abc" && read_s2 == "def") ? "Ok." : "Fail.") << std::endl;
	}
	catch (const DB::Exception & e)
	{
		std::cerr << e.what() << ", " << e.displayText() << std::endl;
		return 1;
	}

	return 0;
}
