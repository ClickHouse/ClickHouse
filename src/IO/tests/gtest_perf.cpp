#include <gtest/gtest.h>

#include <string>
#include <type_traits>
#include <common/StringRef.h>
#include <IO/Operators.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>

using namespace DB;

TEST(TestPerf, qqqq)
{
    constexpr auto N = 10000000;
    WriteBufferFromOwnString ss;
    for (size_t i = 0; i < N; ++i)
        ss << rand() << "\n";

    auto str = ss.str();
    ReadBufferFromMemory buf(str.data(), str.size());

    auto start = clock();

    for (size_t i = 0; i < N; ++i)
    {
        UInt64 x;
        readIntTextUnsafe(x, buf);
        assertChar('\n', buf);
    }

    std::cerr << "time: " <<  static_cast<double>(clock() - start) / CLOCKS_PER_SEC << "\n";
}
