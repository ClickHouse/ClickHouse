#include <iostream>
#include <stdexcept>
#include <Common/ThreadPool.h>


int main(int, char **)
{
    ThreadPool pool(10);

    pool.schedule([]{ throw std::runtime_error("Hello, world!"); });

    try
    {
        while (true)
            pool.schedule([]{});    /// An exception will be rethrown from this method.
    }
    catch (const std::runtime_error & e)
    {
        std::cerr << e.what() << "\n";
    }

    pool.wait();

    return 0;
}
