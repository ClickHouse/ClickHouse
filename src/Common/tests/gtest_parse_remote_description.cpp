#include <Common/parseRemoteDescription.h>
#include <Common/Exception.h>
#include <Common/Stopwatch.h>

#include <gtest/gtest.h>

using namespace DB;

static std::vector<String> parse(const String & description, char separator = ',', size_t max_addresses = 1000)
{
    return parseRemoteDescription(description, 0, description.size(), separator, max_addresses);
}

TEST(ParseRemoteDescription, PlainHost)
{
    EXPECT_EQ(parse("host1"), (std::vector<String>{"host1"}));
    EXPECT_EQ(parse(""), (std::vector<String>{""}));
}

TEST(ParseRemoteDescription, Separator)
{
    EXPECT_EQ(parse("host1,host2"), (std::vector<String>{"host1", "host2"}));
    EXPECT_EQ(parse("host1|host2", '|'), (std::vector<String>{"host1", "host2"}));
}

TEST(ParseRemoteDescription, NumericInterval)
{
    EXPECT_EQ(parse("abc{8..10}def"), (std::vector<String>{"abc8def", "abc9def", "abc10def"}));
    EXPECT_EQ(parse("abc{08..10}def"), (std::vector<String>{"abc08def", "abc09def", "abc10def"}));
}

TEST(ParseRemoteDescription, Enumeration)
{
    EXPECT_EQ(parse("abc{x,yy,z}def"), (std::vector<String>{"abcxdef", "abcyydef", "abczdef"}));
}

TEST(ParseRemoteDescription, CartesianProduct)
{
    EXPECT_EQ(parse("a{1..2}b{x,y}"), (std::vector<String>{"a1bx", "a1by", "a2bx", "a2by"}));
    EXPECT_EQ(parse("{1..2}{3..4}"), (std::vector<String>{"13", "14", "23", "24"}));
}

TEST(ParseRemoteDescription, TooManyAddresses)
{
    EXPECT_THROW(parse("a{1..100}b{1..100}", ',', 10), Exception);
    EXPECT_THROW(parse("a{1..100}", ',', 10), Exception);
}

TEST(ParseRemoteDescription, LongDescription)
{
    /// Parsing used to rebuild the accumulated strings once per character,
    /// which made it quadratic in the description length: a fuzzed 1 MiB
    /// zero-padded FixedString address hung the hung check in the stress test.
    /// The time bound is what catches the regression: at 1 MiB the quadratic
    /// version needs minutes (263 s under ASan) while the linear one needs
    /// well under a second, so the bound has an order-of-magnitude margin
    /// on both sides even under sanitizers.
    String long_host = "127.0.0.1:9004";
    long_host.resize(1 << 20, '\0');
    Stopwatch watch;
    const auto res = parse(long_host, '|');
    EXPECT_LT(watch.elapsedSeconds(), 30);
    ASSERT_EQ(res.size(), 1);
    EXPECT_EQ(res[0], long_host);
}

TEST(ParseRemoteDescription, OverLimitExpansionWithLongSuffix)
{
    /// An oversized brace expansion followed by a long suffix must be rejected
    /// up front, before the suffix has a chance to be appended to every
    /// generated address. The numeric interval and the enumeration are
    /// rejected by different checks, so both are exercised.
    String interval = "{1..1001}";
    interval.resize(1 << 20, 'x');
    Stopwatch watch;
    EXPECT_THROW(parse(interval), Exception);
    EXPECT_LT(watch.elapsedSeconds(), 5);

    String enumeration = "{";
    for (size_t i = 0; i < 1001; ++i)
    {
        if (i != 0)
            enumeration += ',';
        enumeration += std::to_string(i);
    }
    enumeration += '}';
    enumeration.resize(1 << 20, 'x');
    watch.restart();
    EXPECT_THROW(parse(enumeration), Exception);
    EXPECT_LT(watch.elapsedSeconds(), 5);
}

TEST(ParseRemoteDescription, ExternalDatabase)
{
    using Addresses = std::vector<std::pair<String, UInt16>>;
    EXPECT_EQ(parseRemoteDescriptionForExternalDatabase("host1:5432|host2:5433", 10, 5432), (Addresses{{"host1", 5432}, {"host2", 5433}}));
    EXPECT_EQ(parseRemoteDescriptionForExternalDatabase("host1", 10, 5432), (Addresses{{"host1", 5432}}));
    EXPECT_EQ(parseRemoteDescriptionForExternalDatabase("[2001:db8::1]:5432", 10, 5432), (Addresses{{"2001:db8::1", 5432}}));
}
