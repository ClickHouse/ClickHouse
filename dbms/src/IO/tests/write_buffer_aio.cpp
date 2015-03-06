#include <DB/IO/WriteBufferAIO.h>

#include <iostream>
#include <fstream>
#include <streambuf>
#include <cstdlib>

static const size_t BLOCK_SIZE = 512;

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
		buf.reserve(10 * BLOCK_SIZE);
		for (size_t i = 0; i < (10 * BLOCK_SIZE); ++i)
		{
			buf.append(1, source[i % source.length()]);
		}

		// Write data.
		{
			DB::WriteBufferAIO out(filename, 3 * BLOCK_SIZE);
			out.write(&buf[0], buf.length());
		}

		// Read data synchronously.
		std::ifstream in(filename.c_str());
		if (!in.is_open())
			die("Could not open file");

		std::string received{ std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>() };
		ok = (received == buf);
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
