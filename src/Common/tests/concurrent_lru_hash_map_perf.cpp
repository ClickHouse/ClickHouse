#include <iostream>
#include <vector>
#include <thread>
#include <random>
#include <pcg_random.hpp>

#include <Common/HashTable/ConcurrentLRUHashMap.h>
#include <Common/HashTable/LRUHashMap.h>
#include <Common/Stopwatch.h>

std::vector<UInt64> generateNumbersToInsert(size_t numbers_to_insert_size)
{
    std::vector<UInt64> numbers;
    numbers.reserve(numbers_to_insert_size);

    std::random_device rd;
    pcg64 gen(rd());

    UInt64 min = std::numeric_limits<UInt64>::min();
    UInt64 max = std::numeric_limits<UInt64>::max();

    auto distribution = std::uniform_int_distribution<>(min, max);

    for (size_t i = 0; i < numbers_to_insert_size; ++i)
    {
        UInt64 number = distribution(gen);
        numbers.emplace_back(number);
    }

    return numbers;
}

int main(int argc, char ** argv)
{
    (void)(argc);
    (void)(argv);

    std::shared_mutex mtx;
    ConcurrentLRUHashMap<UInt64, UInt64> cache(1000000);

    Stopwatch watch;
    watch.start();

    size_t thread_count = 8;
    std::vector<std::thread> threads;

    for (size_t i = 0; i < thread_count; ++i)
    {
        threads.emplace_back([&]
        {
            std::unique_lock<std::shared_mutex> lock(mtx);
            auto numbers = generateNumbersToInsert(2000000);

            for (auto number : numbers)
                cache.insert(number, number);
        });
    }

    for (auto & thread : threads)
        thread.join();

    threads.clear();

    std::cerr << "Insert take " << watch.elapsedMilliseconds() << " milliseconds size " << cache.size() << std::endl;

    for (size_t count = 0; count < 120; ++count)
    {
        watch.restart();

        size_t summ = 0;

        for (size_t i = 0; i < thread_count; ++i)
        {
            threads.emplace_back([&]
            {
                auto numbers = generateNumbersToInsert(1000000);

                for (auto number : numbers)
                {
                    std::shared_lock<std::shared_mutex> lock(mtx);
                    const auto * value = cache.find(number);

                    if (value != nullptr)
                        summ += *value;
                }
            });
        }

        for (auto & thread : threads)
            thread.join();

        threads.clear();

        std::cerr << "Read take " << watch.elapsedMilliseconds() << " milliseconds " << std::endl;
        std::cerr << "Summ " << summ << std::endl;
    }

    return 0;
}
