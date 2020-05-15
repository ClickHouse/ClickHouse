#include <Common/MemoryStatisticsOS.h>
#include <iostream>


int main(int argc, char ** argv)
{
    using namespace DB;

    size_t num_iterations = argc >= 2 ? std::stoull(argv[1]) : 1000000;
    MemoryStatisticsOS stats;

    uint64_t counter = 0;
    for (size_t i = 0; i < num_iterations; ++i)
    {
        MemoryStatisticsOS::Data data = stats.get();
        counter += data.resident;
    }

    if (num_iterations)
        std::cerr << (counter / num_iterations) << '\n';
    return 0;
}


