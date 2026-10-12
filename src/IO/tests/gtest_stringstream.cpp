#include <gtest/gtest.h>

#include <sstream>
#include <string>

// There are few places where stringstream is used to pass data to some 3d
// party code.
//
// And there was problems with feeding > INT_MAX to stringstream in libc++,
// this is the regression test for it.
//
// Since that places in Clickhouse can operate on buffers > INT_MAX (i.e.
// WriteBufferFromS3), so it is better to have a test for this in ClickHouse
// too.
TEST(stringstream, INTMAX)
{
    std::stringstream ss;
    ss.exceptions(std::ios::badbit);

    std::string payload(1<<20, 'A');

    // write up to INT_MAX-1MiB
    for (size_t i = 0; i < (2ULL<<30) - payload.size(); i += payload.size())
    {
        ASSERT_NE(ss.tellp(), -1);
        ss.write(payload.data(), payload.size());
        // std::cerr << "i: " << ss.tellp()/1024/1024 << " MB\n";
    }

    ASSERT_NE(ss.tellp(), -1);
    // write up to INT_MAX
    ss.write(payload.data(), payload.size());

    ASSERT_NE(ss.tellp(), -1);
    // write one more 1MiB chunk
    ss.write(payload.data(), payload.size());
}
