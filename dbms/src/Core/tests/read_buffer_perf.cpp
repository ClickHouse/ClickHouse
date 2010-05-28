#include <string>

#include <iostream>
#include <fstream>

#include <DB/Core/ReadBufferFromIStream.h>


int main(int argc, char ** argv)
{
	try
	{
		std::ifstream istr("test");
		DB::ReadBufferFromIStream in(istr);

		DB::Int64 a = 0;
		DB::Float64 b = 0;
		DB::String c, d;

		size_t i = 0;
		while (!in.eof())
		{
			in.readIntText(a);
			in.ignore();
			
			in.readFloatText(b);
			in.ignore();
			
			in.readEscapedString(c);
			in.ignore();
			
			in.readQuotedString(d);
			in.ignore();

			++i;
		}

		std::cout << a << ' ' << b << ' ' << c << '\t' << '\'' << d << '\'' << std::endl;
		std::cout << i << std::endl;
	}
	catch (const DB::Exception & e)
	{
		std::cerr << e.what() << ", " << e.message() << std::endl;
		return 1;
	}

	return 0;
}
