#include <string>

#include <iostream>
#include <sstream>

#include <DB/Core/ReadBufferFromIStream.h>


int main(int argc, char ** argv)
{
	try
	{
		std::stringstream s;
		s << "-123456 123.456 вася пе\\tтя\t'\\'xyz\\\\'";
		DB::ReadBufferFromIStream in(s);

		DB::Int64 a;
		DB::Float64 b;
		DB::String c, d;

		in.readIntText(a);
		in.ignore();
		
		in.readFloatText(b);
		in.ignore();
		
		in.readEscapedString(c);
		in.ignore();
		
		in.readQuotedString(d);

		std::cout << a << ' ' << b << ' ' << c << '\t' << '\'' << d << '\'' << std::endl;
	}
	catch (const DB::Exception & e)
	{
		std::cerr << e.what() << ", " << e.message() << std::endl;
		return 1;
	}

	return 0;
}
