#include <Common/Exception.h>
#include <Common/IntervalKind.h>

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#include <array>

namespace DB::ErrorCodes
{
extern const int INCORRECT_DATA;
}

namespace
{
using DB::IntervalKind;

/// Built from the macro on purpose: the test checks that it lists the kinds in enum order.
constexpr std::array all_kinds = {
#define M(KIND) IntervalKind::Kind::KIND,
    FOR_EACH_INTERVAL_KIND(M)
#undef M
};
}

/// `fromBinary` must accept exactly the bytes of the known kinds.
TEST(IntervalKind, FromBinary)
{
    for (size_t i = 0; i < all_kinds.size(); ++i)
    {
        SCOPED_TRACE(i);
        EXPECT_EQ(IntervalKind(all_kinds[i]).toBinary(), i);
        EXPECT_EQ(IntervalKind::fromBinary(static_cast<UInt8>(i)), all_kinds[i]);
    }

    for (unsigned value = all_kinds.size(); value < 256; ++value)
    {
        SCOPED_TRACE(value);
        EXPECT_THAT(
            [&] { return IntervalKind::fromBinary(static_cast<UInt8>(value)); },
            ::testing::Throws<DB::Exception>(::testing::Property(&DB::Exception::code, DB::ErrorCodes::INCORRECT_DATA)));
    }
}
