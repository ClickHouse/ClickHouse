#include <DB/IO/WriteBufferAIO.h>
#include <DB/Core/Defines.h>

#include <boost/filesystem.hpp>

#include <iostream>
#include <fstream>
#include <streambuf>
#include <cstdlib>

namespace
{

void run();
void die(const std::string & msg);
void run_test(unsigned int num, const std::function<bool()> func);

bool test1();
bool test2();
bool test3();
bool test4();
bool test5();

void run()
{
	const std::vector<std::function<bool()> > tests =
	{
		test1,
		test2,
		test3,
		test4,
		test5
	};

	unsigned int num = 0;
	for (const auto & test : tests)
	{
		++num;
		run_test(num, test);
	}
}

void die(const std::string & msg)
{
	std::cout << msg;
	::exit(EXIT_FAILURE);
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

	size_t n = 10 * DEFAULT_AIO_FILE_BLOCK_SIZE;

	std::string buf;
	buf.reserve(n);

	for (size_t i = 0; i < n; ++i)
		buf += symbols[i % symbols.length()];

	{
		DB::WriteBufferAIO out(filename, 3 * DEFAULT_AIO_FILE_BLOCK_SIZE);

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

	size_t n = 10 * DEFAULT_AIO_FILE_BLOCK_SIZE;

	std::string buf;
	buf.reserve(n);

	for (size_t i = 0; i < n; ++i)
		buf += symbols[i % symbols.length()];

	{
		DB::WriteBufferAIO out(filename, 3 * DEFAULT_AIO_FILE_BLOCK_SIZE);

		if (out.getFileName() != filename)
			return false;
		if (out.getFD() == -1)
			return false;

		out.write(&buf[0], buf.length() / 2);
		out.seek(DEFAULT_AIO_FILE_BLOCK_SIZE, SEEK_CUR);
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
	if (received.substr(buf.length() / 2, DEFAULT_AIO_FILE_BLOCK_SIZE) != std::string(DEFAULT_AIO_FILE_BLOCK_SIZE, '\0'))
		return false;
	if (received.substr(buf.length() / 2 + DEFAULT_AIO_FILE_BLOCK_SIZE) != buf.substr(buf.length() / 2))
		return false;

	return true;
}

bool test3()
{
	namespace fs = boost::filesystem;

	static const std::string symbols = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

	char pattern[] = "/tmp/fileXXXXXX";
	char * dir = ::mkdtemp(pattern);
	if (dir == nullptr)
		die("Could not create directory");

	const std::string directory = std::string(dir);
	const std::string filename = directory + "/foo";

	size_t n = 10 * DEFAULT_AIO_FILE_BLOCK_SIZE;

	std::string buf;
	buf.reserve(n);

	for (size_t i = 0; i < n; ++i)
		buf += symbols[i % symbols.length()];

	{
		DB::WriteBufferAIO out(filename, 3 * DEFAULT_AIO_FILE_BLOCK_SIZE);

		if (out.getFileName() != filename)
			return false;
		if (out.getFD() == -1)
			return false;

		out.write(&buf[0], buf.length());

		off_t pos1 = out.getPositionInFile();

		out.truncate(buf.length() / 2);

		off_t pos2 = out.getPositionInFile();

		if (pos1 != pos2)
			return false;
	}

	std::ifstream in(filename.c_str());
	if (!in.is_open())
		die("Could not open file");

	std::string received{ std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>() };

	in.close();
	fs::remove_all(directory);

	return (received == buf.substr(0, buf.length() / 2));
}

bool test4()
{
	namespace fs = boost::filesystem;

	static const std::string symbols = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

	char pattern[] = "/tmp/fileXXXXXX";
	char * dir = ::mkdtemp(pattern);
	if (dir == nullptr)
		die("Could not create directory");

	const std::string directory = std::string(dir);
	const std::string filename = directory + "/foo";

	size_t n = 10 * DEFAULT_AIO_FILE_BLOCK_SIZE;

	std::string buf;
	buf.reserve(n);

	for (size_t i = 0; i < n; ++i)
		buf += symbols[i % symbols.length()];

	{
		DB::WriteBufferAIO out(filename, 3 * DEFAULT_AIO_FILE_BLOCK_SIZE);

		if (out.getFileName() != filename)
			return false;
		if (out.getFD() == -1)
			return false;

		out.write(&buf[0], buf.length());

		off_t pos1 = out.getPositionInFile();

		out.truncate(3 * buf.length() / 2);

		off_t pos2 = out.getPositionInFile();

		if (pos1 != pos2)
			return false;
	}

	std::ifstream in(filename.c_str());
	if (!in.is_open())
		die("Could not open file");

	std::string received{ std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>() };

	in.close();
	fs::remove_all(directory);

	if (received.substr(0, buf.length()) != buf)
		return false;

	if (received.substr(buf.length()) != std::string(buf.length() / 2, '\0'))
		return false;

	return true;
}

bool test5()
{
	namespace fs = boost::filesystem;

	static const std::string symbols = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

	char pattern[] = "/tmp/fileXXXXXX";
	char * dir = ::mkdtemp(pattern);
	if (dir == nullptr)
		die("Could not create directory");

	const std::string directory = std::string(dir);
	const std::string filename = directory + "/foo";

	size_t n = 10 * DEFAULT_AIO_FILE_BLOCK_SIZE;

	std::string buf;
	buf.reserve(n);

	for (size_t i = 0; i < n; ++i)
		buf += symbols[i % symbols.length()];

	{
		DB::WriteBufferAIO out(filename, 3 * DEFAULT_AIO_FILE_BLOCK_SIZE);

		if (out.getFileName() != filename)
			return false;
		if (out.getFD() == -1)
			return false;

		out.seek(1, SEEK_SET);
		out.write(&buf[0], buf.length());
	}

	std::ifstream in(filename.c_str());
	if (!in.is_open())
		die("Could not open file");

	std::string received{ std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>() };

	in.close();
	fs::remove_all(directory);

	return received.substr(1) == buf;
}

}

int main()
{
	run();
	return 0;
}
