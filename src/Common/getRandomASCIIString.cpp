#include <Common/getRandomASCIIString.h>
#include <Common/thread_local_rng.h>
#include <random>

namespace DB
{

static constexpr char ASCIIChars[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

String getRandomASCIIString(size_t length)
{
    return getRandomASCIIString(length, thread_local_rng);
}

String getRandomASCIIString(size_t length, pcg64 & rng)
{
    // Minus 2 to exclude the null terminator in `ASCIIChars`
    std::uniform_int_distribution<int> distribution(0, sizeof(ASCIIChars) - 2);
    String res;
    res.resize(length);
    for (auto & c : res)
        c = ASCIIChars[distribution(rng)];
    return res;
}

}
