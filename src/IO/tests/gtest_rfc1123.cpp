#include <gtest/gtest.h>

#include <common/DateLUT.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>


TEST(RFC1123, Test)
{
    using namespace DB;
    WriteBufferFromOwnString out;
    writeDateTimeTextRFC1123(1111111111, out, DateLUT::instance("UTC"));
    ASSERT_EQ(out.str(), "Fri, 18 Mar 2005 01:58:31 GMT");
}
