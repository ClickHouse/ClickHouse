#include <iostream>
#include <vector>
#include <thread>

#include <Common/HashTable/ConcurrentLRUHashMap.h>

int main(int argc, char ** argv)
{
    (void)(argc);
    (void)(argv);

    ConcurrentLRUHashMap<int, int> cache(200000);

    size_t thread_count = 32;
    std::vector<std::thread> insert_threads;

    for (size_t i = 0; i < thread_count; ++i)
    {
        insert_threads.emplace_back([&]
        {
            for (size_t index = 0; index < 1000000; ++index)
                cache.insert(index, index);
        });
    }

    for (auto & insert_thread : insert_threads)
    {
        insert_thread.join();
    }

    cache.forEach([](auto & key, auto & value)
    {
        std::cerr << "Key " << key << " value " << value << std::endl;
    });

    return 0;
}
