#include <Common/SipHash.h>

#include <string_view>

#include <gtest/gtest.h>

/// Regression test for an UndefinedBehaviorSanitizer finding at SipHash.h
/// reported by the AST fuzzer (STID 3285-3bba): "applying non-zero offset 8 to
/// null pointer" inside `SipHash::update(const char *, UInt64)`. The trigger
/// path was `SerializationInterval::getHash` -> `SipHash::update(string_view)`
/// where the string_view came from `magic_enum::enum_name` on an out-of-range
/// `IntervalKind::Kind` and therefore had `data() == nullptr`, `size() == 0`.
TEST(SipHash, UpdateWithEmptyStringViewDoesNotInvokeUB)
{
    /// A default-constructed string_view has `data() == nullptr, size() == 0`
    /// on libcxx (which ClickHouse uses).
    std::string_view empty;
    ASSERT_EQ(empty.data(), nullptr);
    ASSERT_EQ(empty.size(), 0u);

    SipHash hash;
    hash.update(empty);                              /// must not invoke UB
    hash.update(std::string_view{"Interval"});       /// follow-up payload
    hash.update(empty);                              /// trailing empty: still safe
    (void)hash.get128();
}

/// Empty input must be a true no-op: hashing X is equal to hashing
/// "" + X + "" with empty updates intermixed.
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
