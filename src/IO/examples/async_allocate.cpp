#include <iostream>
#include <thread>
#include <chrono>
#include <vector>
#include <array>
#include <orc/MemoryPool.hh>
#include <Common/Stopwatch.h>
#include <benchmark/benchmark.h>

static constexpr size_t numThreads = 100;
static constexpr size_t bufferSize = 1024 * 1024;
// static std::vector<std::thread> threads1(numThreads);
// static std::vector<std::thread> threads2(numThreads);

static void allocateAndWriteInBackground()
{
    for (size_t i = 0; i < numThreads; ++i)
    {
        threads1[i] = std::thread(
            []()
            {
                orc::DataBuffer<char> buffer(*orc::getDefaultPool(), bufferSize);
                for (size_t j = 0; j < bufferSize; ++j)
                    buffer[j] = static_cast<char>(j);
            });
    }

    for (auto & thread : threads1)
    {
        thread.join();
    }
}

static void writeInBackground()
{
    std::vector<std::shared_ptr<orc::DataBuffer<char>>> buffers(numThreads);
    for (size_t i = 0; i < numThreads; ++i)
    {
        auto buffer = std::make_shared<orc::DataBuffer<char>>(*orc::getDefaultPool(), bufferSize);
        buffers[i] = buffer;
        threads2[i] = std::thread(
            [&buffer]()
            {
                for (size_t j = 0; j < bufferSize; ++j)
                    (*buffer)[j] = static_cast<char>(j);
            });
    }

    for (auto & thread : threads2)
    {
        thread.join();
    }
}

int main(int argc, char ** /*argv*/) {
    Stopwatch watch;

    if (argc == 1)
    {
        watch.restart();
        allocateAndWriteInBackground();
        std::cout << "allocateAndWriteInBackground() took " << watch.elapsedSeconds() << " seconds." << std::endl;
    }
    else
    {
        watch.restart();
        writeInBackground();
        std::cout << "writeInBackground () took " << watch.elapsedSeconds() << " seconds." << std::endl;
    }

    return 0;
}
