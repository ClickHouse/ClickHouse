#include <DB/IO/WriteBufferAIO.h>
#include <boost/filesystem.hpp>

#include <iostream>
#include <fstream>
#include <streambuf>
#include <functional>
#include <cstdlib>

namespace
{

void die(const std::string & msg)
{
	std::cout << msg;
	::exit(EXIT_FAILURE);
}

bool test1()
{
	namespace fs = boost::filesystem;

	static const std::string symbols = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

	char pattern[] = "/tmp/fileXXXXXX";
	char * dir = ::mkdtemp(pattern);
	if (dir == nullptr)
		die("Could not create directory");

	const std::string directory = std::string(dir);
	const std::string filename = directory + "/foo";

	size_t n = 10 * DB::WriteBufferAIO::BLOCK_SIZE;

	std::string buf;
	buf.reserve(n);

	for (size_t i = 0; i < n; ++i)
		buf += symbols[i % symbols.length()];

	{
		DB::WriteBufferAIO out(filename, 3 * DB::WriteBufferAIO::BLOCK_SIZE);

		if (out.getFileName() != filename)
			return false;
		if (out.getFD() == -1)
			return false;

		out.write(&buf[0], buf.length());
	}

	std::ifstream in(filename.c_str());
	if (!in.is_open())
		die("Could not open file");

	std::string received{ std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>() };

	in.close();
	fs::remove_all(directory);

	return (received == buf);
}

bool test2()
{
	namespace fs = boost::filesystem;

	static const std::string symbols = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

	char pattern[] = "/tmp/fileXXXXXX";
	char * dir = ::mkdtemp(pattern);
	if (dir == nullptr)
		die("Could not create directory");

	const std::string directory = std::string(dir);
	const std::string filename = directory + "/foo";

	size_t n = 10 * DB::WriteBufferAIO::BLOCK_SIZE;

	std::string buf;
	buf.reserve(n);

	for (size_t i = 0; i < n; ++i)
		buf += symbols[i % symbols.length()];

	{
		DB::WriteBufferAIO out(filename, 3 * DB::WriteBufferAIO::BLOCK_SIZE);

		if (out.getFileName() != filename)
			return false;
		if (out.getFD() == -1)
			return false;

		out.write(&buf[0], buf.length() / 2);
		out.seek(DB::WriteBufferAIO::BLOCK_SIZE, SEEK_CUR);
		out.write(&buf[buf.length() / 2], buf.length() / 2);
	}

	std::ifstream in(filename.c_str());
	if (!in.is_open())
		die("Could not open file");

	std::string received{ std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>() };

	in.close();
	fs::remove_all(directory);

	if (received.substr(0, buf.length() / 2) != buf.substr(0, buf.length() / 2))
		return false;
	if (received.substr(buf.length() / 2, DB::WriteBufferAIO::BLOCK_SIZE) != std::string(DB::WriteBufferAIO::BLOCK_SIZE, '\0'))
		return false;
	if (received.substr(buf.length() / 2 + DB::WriteBufferAIO::BLOCK_SIZE) != buf.substr(buf.length() / 2))
		return false;

	return true;
}

void run_test(unsigned int num, const std::function<bool()> func)
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
		std::cout << "Test " << num << " passed\n";
	else
		std::cout << "Test " << num << " failed\n";
}

void run()
{
	const std::vector<std::function<bool()> > tests =
	{
		test1,
		test2
	};

	unsigned int num = 0;
	for (const auto & test : tests)
	{
		++num;
		run_test(num, test);
	}
}

}

int main()
{
	run();
	return 0;
}
