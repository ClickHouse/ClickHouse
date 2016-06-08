#include <DB/Common/LRUCache.h>

#include <iostream>
#include <string>
#include <thread>
#include <chrono>

namespace
{

void run();
void runTest(unsigned int num, const std::function<bool()> func);
bool test1();
bool test2();

#define ASSERT_CHECK(cond, res) \
do \
{ \
	if (!(cond)) \
	{ \
		std::cout << __FILE__ << ":" << __LINE__ << ":" \
			<< "Assertion " << #cond << " failed.\n"; \
		if ((res)) { (res) = false; } \
	} \
} \
while (0)

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
		runTest(num, test);
	}
}

void runTest(unsigned int num, const std::function<bool()> func)
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
	catch (...)
	{
		ok = false;
		std::cout << "Caught unhandled exception\n";
	}

	if (ok)
		std::cout << "Test " << num << " passed\n";
	else
		std::cout << "Test " << num << " failed\n";
}

struct Weight
{
	size_t operator()(const std::string & s) const
	{
		return s.size();
	}
};

bool test1()
{
	using Cache = DB::LRUCache<std::string, std::string, std::hash<std::string>, Weight>;
	using MappedPtr = Cache::MappedPtr;

	auto ptr = [](const std::string & s)
	{
		return MappedPtr(new std::string(s));
	};

	Cache cache(10);

	bool res = true;

	ASSERT_CHECK(!cache.get("asd"), res);

	cache.set("asd", ptr("qwe"));

	ASSERT_CHECK((*cache.get("asd") == "qwe"), res);

	cache.set("zxcv", ptr("12345"));
	cache.set("01234567891234567", ptr("--"));

	ASSERT_CHECK((*cache.get("zxcv") == "12345"), res);
	ASSERT_CHECK((*cache.get("asd") == "qwe"), res);
	ASSERT_CHECK((*cache.get("01234567891234567") == "--"), res);
	ASSERT_CHECK(!cache.get("123x"), res);

	cache.set("321x", ptr("+"));

	ASSERT_CHECK(!cache.get("zxcv"), res);
	ASSERT_CHECK((*cache.get("asd") == "qwe"), res);
	ASSERT_CHECK((*cache.get("01234567891234567") == "--"), res);
	ASSERT_CHECK(!cache.get("123x"), res);
	ASSERT_CHECK((*cache.get("321x") == "+"), res);

	ASSERT_CHECK((cache.weight() == 6), res);
	ASSERT_CHECK((cache.count() == 3), res);

	return res;
}

bool test2()
{
	using namespace std::literals;
	using Cache = DB::LRUCache<std::string, std::string, std::hash<std::string>, Weight>;
	using MappedPtr = Cache::MappedPtr;

	auto ptr = [](const std::string & s)
	{
		return MappedPtr(new std::string(s));
	};

	Cache cache(10, 3s);

	bool res = true;

	ASSERT_CHECK(!cache.get("asd"), res);

	cache.set("asd", ptr("qwe"));

	ASSERT_CHECK((*cache.get("asd") == "qwe"), res);

	cache.set("zxcv", ptr("12345"));
	cache.set("01234567891234567", ptr("--"));

	ASSERT_CHECK((*cache.get("zxcv") == "12345"), res);
	ASSERT_CHECK((*cache.get("asd") == "qwe"), res);
	ASSERT_CHECK((*cache.get("01234567891234567") == "--"), res);
	ASSERT_CHECK(!cache.get("123x"), res);

	cache.set("321x", ptr("+"));

	ASSERT_CHECK((cache.get("zxcv")), res);
	ASSERT_CHECK((*cache.get("asd") == "qwe"), res);
	ASSERT_CHECK((*cache.get("01234567891234567") == "--"), res);
	ASSERT_CHECK(!cache.get("123x"), res);
	ASSERT_CHECK((*cache.get("321x") == "+"), res);

	ASSERT_CHECK((cache.weight() == 11), res);
	ASSERT_CHECK((cache.count() == 4), res);

	std::this_thread::sleep_for(5s);

	cache.set("123x", ptr("2769"));

	ASSERT_CHECK(!cache.get("zxcv"), res);
	ASSERT_CHECK((*cache.get("asd") == "qwe"), res);
	ASSERT_CHECK((*cache.get("01234567891234567") == "--"), res);
	ASSERT_CHECK((*cache.get("321x") == "+"), res);

	ASSERT_CHECK((cache.weight() == 10), res);
	ASSERT_CHECK((cache.count() == 4), res);

	return res;
}

}

int main()
{
	run();
	return 0;
}

