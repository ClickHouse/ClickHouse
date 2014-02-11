#include <DB/Common/LRUCache.h>
#include <iostream>
#include <string>

using namespace DB;

struct Weight
{
	size_t operator()(const std::string & s) const
	{
		return s.size();
	}
};

void fail()
{
	std::cout << "failed" << std::endl;
	exit(1);
}

typedef LRUCache<std::string, std::string, std::hash<std::string>, Weight> Cache;
typedef Cache::MappedPtr MappedPtr;

MappedPtr ptr(const std::string & s)
{
	return MappedPtr(new std::string(s));
}

int main()
{
	try
	{
		Cache cache(10);

		if (cache.get("asd")) fail();
		cache.set("asd", ptr("qwe"));
		if (*cache.get("asd") != "qwe") fail();
		cache.set("zxcv", ptr("12345"));
		cache.set("01234567891234567", ptr("--"));
		if (*cache.get("zxcv") != "12345") fail();
		if (*cache.get("asd") != "qwe") fail();
		if (*cache.get("01234567891234567") != "--") fail();
		if (cache.get("123x")) fail();
		cache.set("321x", ptr("+"));
		if (cache.get("zxcv")) fail();

		if (*cache.get("asd") != "qwe") fail();
		if (*cache.get("01234567891234567") != "--") fail();
		if (cache.get("123x")) fail();
		if (*cache.get("321x") != "+") fail();

		if (cache.weight() != 6) fail();
		if (cache.count() != 3) fail();

		std::cout << "passed" << std::endl;
	}
	catch (...)
	{
		fail();
	}

	return 0;
}
