#include <string>

#include <iostream>
#include <fstream>

#include <DB/Core/Types.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/ReadBufferFromIStream.h>
#include <DB/IO/CompressedReadBuffer.h>


int main(int argc, char ** argv)
{
	try
	{
		std::ifstream istr("DevicePixelRatio");
		DB::ReadBufferFromIStream in(istr);

		DB::Float32 b = 0;

		size_t i = 0;
		while (!in.eof())
		{
			DB::readFloatText(b, in);
			in.ignore();

			++i;
		}

		std::cout << b << std::endl;
		std::cout << i << std::endl;
	}
	catch (const DB::Exception & e)
	{
		std::cerr << e.what() << ", " << e.displayText() << std::endl;
		return 1;
	}

	return 0;
}
