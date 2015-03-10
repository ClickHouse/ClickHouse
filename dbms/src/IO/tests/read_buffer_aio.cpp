#include <DB/IO/ReadBufferAIO.h>

#include <iostream>
#include <fstream>
#include <functional>
#include <cstdlib>
#include <unistd.h>

namespace
{

void run();
void prepare(std::string  & filename, std::string & buf);
void die(const std::string & msg);
void run_test(std::function<bool()> && func);
bool test1(const std::string & filename, const std::string & buf);
bool test2(const std::string & filename, const std::string & buf);

void run()
{
	std::string filename;
	std::string buf;
	prepare(filename, buf);

	run_test(std::bind(test1, std::ref(filename), std::ref(buf)));
	run_test(std::bind(test2, std::ref(filename), std::ref(buf)));

	::unlink(filename.c_str());
}

void prepare(std::string  & filename, std::string & buf)
{
	static const std::string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

	char pattern[] = "/tmp/fileXXXXXX";
	char * dir = ::mkdtemp(pattern);
	if (dir == nullptr)
		die("Could not create directory");

	filename = std::string(dir) + "/foo";

	size_t n = 10 * DB::ReadBufferAIO::BLOCK_SIZE;
	buf.reserve(n);

	for (size_t i = 0; i < n; ++i)
		buf += chars[i % chars.length()];

	std::ofstream out(filename.c_str());
	if (!out.is_open())
		die("Could not open file");

	out << buf;
}

void die(const std::string & msg)
{
	std::cout << msg << "\n";
	::exit(EXIT_FAILURE);
}

void run_test(std::function<bool()> && func)
{
	bool ok;

	try
	{
		ok = func();
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
}

bool test1(const std::string & filename, const std::string & buf)
{
	std::string newbuf;
	newbuf.resize(buf.length());

	DB::ReadBufferAIO in(filename, 3 * DB::ReadBufferAIO::BLOCK_SIZE);
	(void) in.read(&newbuf[0], newbuf.length());

	return (newbuf == buf);
}

bool test2(const std::string & filename, const std::string & buf)
{
	std::string newbuf;
	newbuf.resize(buf.length());

	size_t requested = 9 * DB::ReadBufferAIO::BLOCK_SIZE;

	DB::ReadBufferAIO in(filename, 3 * DB::ReadBufferAIO::BLOCK_SIZE);
	in.setMaxBytes(requested);
	size_t n_read = in.read(&newbuf[0], newbuf.length());

	newbuf.resize(n_read);
	return (newbuf == buf.substr(0, requested));
}

}

int main()
{
	run();
	return 0;
}
