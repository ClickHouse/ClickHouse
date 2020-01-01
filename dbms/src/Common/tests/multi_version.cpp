#include <string.h>
#include <iostream>
#include <Common/ThreadPool.h>
#include <functional>
#include <Common/MultiVersion.h>
#include <Poco/Exception.h>


using T = std::string;
using MV = MultiVersion<T>;
using Results = std::vector<T>;


static void thread1(MV & x, T & result)
{
    MV::Version v = x.get();
    result = *v;
}

static void thread2(MV & x, const char * result)
{
    x.set(std::make_unique<T>(result));
}


int main(int, char **)
{
    try
    {
        const char * s1 = "Hello!";
        const char * s2 = "Goodbye!";

        size_t n = 1000;
        MV x(std::make_unique<T>(s1));
        Results results(n);

        ThreadPool tp(8);
        for (size_t i = 0; i < n; ++i)
        {
            tp.scheduleOrThrowOnError(std::bind(thread1, std::ref(x), std::ref(results[i])));
            tp.scheduleOrThrowOnError(std::bind(thread2, std::ref(x), (rand() % 2) ? s1 : s2));
        }
        tp.wait();

        for (size_t i = 0; i < n; ++i)
            std::cerr << results[i] << " ";
        std::cerr << std::endl;
    }
    catch (const Poco::Exception & e)
    {
        std::cerr << e.message() << std::endl;
        throw;
    }

    return 0;
}
