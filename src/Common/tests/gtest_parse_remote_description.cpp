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

TEST(ParseRemoteDescription, Quirks)
{
    /// An empty part generates nothing rather than an empty address, and so does an empty group.
    EXPECT_EQ(parse("host1,,host2"), (std::vector<String>{"host1", "host2"}));
    EXPECT_EQ(parse("host1,"), (std::vector<String>{"host1"}));
    EXPECT_EQ(parse(",host1"), (std::vector<String>{"host1"}));
    EXPECT_EQ(parse("a{,}b"), (std::vector<String>{"ab"}));

    /// A group without the current separator inside is copied verbatim, braces included: it is
    /// expanded by the second pass, the one that splits replicas by '|'.
    EXPECT_EQ(parse("abc{x|yy|z}def"), (std::vector<String>{"abc{x|yy|z}def"}));
    EXPECT_EQ(parse("abc{x|yy|z}def", '|'), (std::vector<String>{"abcxdef", "abcyydef", "abczdef"}));

    /// Nested groups are flattened.
    EXPECT_EQ(parse("{a,{b,c}}"), (std::vector<String>{"a", "b", "c"}));

    /// Leading zeroes are added only when both borders are written with the same width.
    EXPECT_EQ(parse("{0..10}"), (std::vector<String>{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}));
    EXPECT_EQ(parse("{08..11}"), (std::vector<String>{"08", "09", "10", "11"}));
}

static std::vector<String> generate(RemoteDescriptionGenerator & generator, size_t count)
{
    std::vector<String> res;
    String address;
    while (res.size() < count && generator.next(address))
        res.push_back(address);
    return res;
}

TEST(RemoteDescriptionGenerator, GeneratesTheSameAddresses)
{
    const String description = "a{1..2}b{x,y}";
    RemoteDescriptionGenerator generator(description, 0, description.size(), ',', 1000);
    EXPECT_EQ(generator.totalCount(), std::optional<UInt64>(4));
    EXPECT_EQ(generate(generator, 100), (std::vector<String>{"a1bx", "a1by", "a2bx", "a2by"}));
}

TEST(RemoteDescriptionGenerator, DoesNotMaterializeTheProduct)
{
    /// A hundred million addresses: `parseRemoteDescription` cannot serve this pattern at any limit
    /// that fits in memory, but a reader that stops early only pays for what it takes.
    const String description = "a{0..10000}b{0..10000}";
    RemoteDescriptionGenerator generator(description, 0, description.size(), ',', 1000);

    EXPECT_EQ(generator.totalCount(), std::optional<UInt64>(100020001));
    Stopwatch watch;
    EXPECT_EQ(generate(generator, 3), (std::vector<String>{"a0b0", "a0b1", "a0b2"}));
    EXPECT_LT(watch.elapsedSeconds(), 5);

    EXPECT_THROW(parse(description, ',', 1000), Exception);
}

TEST(RemoteDescriptionGenerator, StopsAtMaxAddresses)
{
    /// The limit is on what the pattern is allowed to produce, so it is only hit by a reader that
    /// asks for more than that.
    const String description = "a{0..10000}";
    RemoteDescriptionGenerator generator(description, 0, description.size(), ',', 10);

    EXPECT_EQ(generate(generator, 10).size(), 10);

    String address;
    EXPECT_THROW(generator.next(address), Exception);
}

TEST(RemoteDescriptionGenerator, ExhaustedExactlyAtMaxAddresses)
{
    /// Producing exactly `max_addresses` addresses is allowed; the generator must report exhaustion
    /// instead of throwing at the end.
    const String description = "a{1..10}";
    RemoteDescriptionGenerator generator(description, 0, description.size(), ',', 10);

    EXPECT_EQ(generate(generator, 100).size(), 10);

    String address;
    EXPECT_FALSE(generator.next(address));
}

TEST(RemoteDescriptionGenerator, TotalCountOverflow)
{
    /// The product does not fit into UInt64, which every caller has to handle explicitly.
    const String description = "{0..1000000000000000}{0..1000000000000000}";
    RemoteDescriptionGenerator generator(description, 0, description.size(), ',', 1000);

    EXPECT_EQ(generator.totalCount(), std::nullopt);
    EXPECT_THROW(parse(description, ',', 1000), Exception);
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
