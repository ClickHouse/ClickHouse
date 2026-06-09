#include <Common/SipHash.h>

#include <string_view>

#include <gtest/gtest.h>

TEST(SipHash, UpdateWithEmptyStringViewDoesNotInvokeUB)
{
    std::string_view empty;

    SipHash hash;
    hash.update(empty);
    hash.update(std::string_view{"Interval"});
    hash.update(empty);
    (void)hash.get128();
}

TEST(SipHash, UpdateWithEmptyStringViewIsNoOp)
{
    const std::string_view payload{"Interval"};

    SipHash baseline;
    baseline.update(payload);
    UInt128 baseline_value = baseline.get128();

    SipHash with_empty;
    with_empty.update(std::string_view{});
    with_empty.update(payload);
    with_empty.update(std::string_view{});
    UInt128 with_empty_value = with_empty.get128();

    EXPECT_EQ(baseline_value, with_empty_value);
}
