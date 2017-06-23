#include <string.h>
#include <iostream>
#include <common/ThreadPool.h>
#include <functional>
#include <common/MultiVersion.h>
#include <Poco/Exception.h>


using T = std::string;
using MV = MultiVersion<T>;
using Results = std::vector<T>;


void thread1(MV & x, T & result)
{
    MV::Version v = x.get();
    result = *v;
}

void thread2(MV & x, const char * result)
{
    x.set(std::make_shared<T>(result));
}


int main(int argc, char ** argv)
{
    try
    {
        const char * s1 = "Hello!";
        const char * s2 = "Goodbye!";

        size_t n = 1000;
        MV x(std::make_shared<T>(s1));
        Results results(n);

        ThreadPool tp(8);
        for (size_t i = 0; i < n; ++i)
        {
            tp.schedule(std::bind(thread1, std::ref(x), std::ref(results[i])));
            tp.schedule(std::bind(thread2, std::ref(x), (rand() % 2) ? s1 : s2));
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
