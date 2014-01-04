#include <string>

#include <iostream>

#include <DB/Core/Types.h>
#include <DB/IO/WriteHelpers.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteBufferFromFile.h>
#include <DB/IO/ReadBufferFromFile.h>


int main(int argc, char ** argv)
{
	using namespace DB;

	try
	{
		ReadBufferFromFile rand_in("/dev/urandom");
		unsigned rand = 0;
		readBinary(rand, rand_in);

		String test = "Hello, world! " + toString(rand);

		{
			WriteBufferFromFile wb("test", 4096);
			writeStringBinary(test, wb);
			wb.next();
		}

		{
			ReadBufferFromFile rb("test", 4096, O_RDONLY | O_DIRECT, nullptr, 4096);
			String res;
			readStringBinary(res, rb);

			std::cerr << "test: " << test << ", res: " << res << std::endl;
		}
	}
	catch (const Exception & e)
	{
		std::cerr << e.what() << ", " << e.displayText() << std::endl;
		return 1;
	}

	return 0;
}
