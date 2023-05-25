#include <Common/getRandomASCIIString.h>
#include <Common/thread_local_rng.h>
#include <random>

namespace DB
{

String getRandomASCIIString(size_t length)
{
    std::uniform_int_distribution<int> distribution('a', 'z');
    String res;
    res.resize(length);
    for (auto & c : res)
        c = distribution(thread_local_rng);
    return res;
}

}
