#include <Common/LRUCache.h>
#include <Common/Exception.h>

#include <iostream>
#include <string>
#include <thread>
#include <chrono>
#include <functional>


namespace
{

void run();
void runTest(unsigned int num, const std::function<bool()> & func);
bool test1();
bool test2();
bool test_concurrent();

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
    const std::vector<std::function<bool()>> tests =
    {
        test1,
        test2,
        test_concurrent
    };

    unsigned int num = 0;
    for (const auto & test : tests)
    {
        ++num;
        runTest(num, test);
    }
}

void runTest(unsigned int num, const std::function<bool()> & func)
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

bool test_concurrent()
{
    using namespace std::literals;

    using Cache = DB::LRUCache<std::string, std::string, std::hash<std::string>, Weight>;
    Cache cache(2);

    bool res = true;

    auto load_func = [](const std::string & result, std::chrono::seconds sleep_for, bool throw_exc)
    {
        std::this_thread::sleep_for(sleep_for);
        if (throw_exc)
            throw std::runtime_error("Exception!");
        return std::make_shared<std::string>(result);
    };

    /// Case 1: Both threads are able to load the value.

    std::pair<Cache::MappedPtr, bool> result1;
    std::thread thread1([&]()
    {
        result1 = cache.getOrSet("key", [&]() { return load_func("val1", 1s, false); });
    });

    std::pair<Cache::MappedPtr, bool> result2;
    std::thread thread2([&]()
    {
        result2 = cache.getOrSet("key", [&]() { return load_func("val2", 1s, false); });
    });

    thread1.join();
    thread2.join();

    ASSERT_CHECK((result1.first == result2.first), res);
    ASSERT_CHECK((result1.second != result2.second), res);

    /// Case 2: One thread throws an exception during loading.

    cache.reset();

    bool thrown = false;
    thread1 = std::thread([&]()
    {
        try
        {
            cache.getOrSet("key", [&]() { return load_func("val1", 2s, true); });
        }
        catch (...)
        {
            thrown = true;
        }
    });

    thread2 = std::thread([&]()
    {
        std::this_thread::sleep_for(1s);
        result2 = cache.getOrSet("key", [&]() { return load_func("val2", 1s, false); });
    });

    thread1.join();
    thread2.join();

    ASSERT_CHECK((thrown == true), res);
    ASSERT_CHECK((result2.second == true), res);
    ASSERT_CHECK((result2.first.get() == cache.get("key").get()), res);
    ASSERT_CHECK((*result2.first == "val2"), res);

    /// Case 3: All threads throw an exception.

    cache.reset();

    bool thrown1 = false;
    thread1 = std::thread([&]()
    {
        try
        {
            cache.getOrSet("key", [&]() { return load_func("val1", 1s, true); });
        }
        catch (...)
        {
            thrown1 = true;
        }
    });

    bool thrown2 = false;
    thread2 = std::thread([&]()
    {
        try
        {
            cache.getOrSet("key", [&]() { return load_func("val1", 1s, true); });
        }
        catch (...)
        {
            thrown2 = true;
        }
    });

    thread1.join();
    thread2.join();

    ASSERT_CHECK((thrown1 == true), res);
    ASSERT_CHECK((thrown2 == true), res);
    ASSERT_CHECK((cache.get("key") == nullptr), res);

    /// Case 4: Concurrent reset.

    cache.reset();

    thread1 = std::thread([&]()
    {
        result1 = cache.getOrSet("key", [&]() { return load_func("val1", 2s, false); });
    });

    std::this_thread::sleep_for(1s);
    cache.reset();

    thread1.join();

    ASSERT_CHECK((result1.second == true), res);
    ASSERT_CHECK((*result1.first == "val1"), res);
    ASSERT_CHECK((cache.get("key") == nullptr), res);

    return res;
}

}

int main()
{
    run();
    return 0;
}

