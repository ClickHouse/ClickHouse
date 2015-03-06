#include <DB/IO/ReadBufferAIO.h>

#include <iostream>
#include <fstream>
#include <streambuf>
#include <algorithm>
#include <cstdlib>

static const std::string source = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

void die(const std::string & msg)
{
	std::cout << msg;
	::exit(EXIT_FAILURE);
}

int main()
{
	bool ok;

	try
	{
		// Create temporary directory and file inside it.
		char pattern[] = "/tmp/fileXXXXXX";
		char * dir = ::mkdtemp(pattern);
		if (dir == nullptr)
			die("Could not create directory");

		const std::string filename = std::string(dir) + "/foo";

		// Create data.
		std::string buf;
		buf.reserve(10 * DB::ReadBufferAIO::BLOCK_SIZE);
		for (size_t i = 0; i < (10 * DB::ReadBufferAIO::BLOCK_SIZE); ++i)
		{
			buf.append(1, source[i % source.length()]);
		}

		// Write data synchrounously.
		{
			std::ofstream out(filename.c_str());
			if (!out.is_open())
				die("Could not open file");

			out << buf;
		}

		// Read data.
		std::vector<char> newbuf(3 * DB::ReadBufferAIO::BLOCK_SIZE);
		{
			DB::ReadBufferAIO in(filename, 3 * DB::ReadBufferAIO::BLOCK_SIZE);
			in.read(&newbuf[0], newbuf.size());
		}

		ok = std::equal(newbuf.begin(), newbuf.end(), buf.begin());
	}
	catch (const DB::Exception & ex)
	{
		ok = false;
		std::cout << "Caught exception " << ex.displayText() << "\n";
	}
	catch (const std::exception & ex)
	{
		ok = false;
		std::cout << "Caught exception " << ex.what() << "\n";
	}

	if (ok)
		std::cout << "Test passed\n";
	else
		std::cout << "Test failed\n";

	return 0;
}
