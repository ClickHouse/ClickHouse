#include <Common/parseRemoteDescription.h>
#include <Common/Exception.h>
#include <Common/Stopwatch.h>

#include <gmock/gmock.h>
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

TEST(ParseRemoteDescription, TooManyAddressesMessage)
{
    /// The message has to name the surface that was actually invoked, how many addresses the pattern
    /// generates and the setting that raises the limit, otherwise there is nothing to act on.
    auto message = [](const String & description, size_t max_addresses, const RemoteDescriptionCaller & caller)
    {
        try
        {
            parseRemoteDescription(description, 0, description.size(), ',', max_addresses, caller);
        }
        catch (const Exception & e)
        {
            return String(e.message());
        }
        return String("no exception");
    };

    /// A single numeric interval over the limit.
    const auto interval = message("a{1..100}", 10, urlCaller("Table function 'url'", "'s3' (or another object storage table function)"));
    EXPECT_THAT(interval, testing::HasSubstr("Table function 'url'"));
    EXPECT_THAT(interval, testing::HasSubstr("too many result addresses: 100, while at most 10 are allowed"));
    EXPECT_THAT(interval, testing::HasSubstr("'glob_expansion_max_elements' setting"));
    /// For `url` the message also explains why the very same pattern is accepted by `s3`.
    EXPECT_THAT(interval, testing::HasSubstr("'s3'"));

    /// A direct product over the limit: neither of the two intervals exceeds it on its own.
    const auto product = message("a{1..4}b{1..4}", 10, urlCaller("Table function 'url'", "'s3' (or another object storage table function)"));
    EXPECT_THAT(product, testing::HasSubstr("Table function 'url'"));
    EXPECT_THAT(product, testing::HasSubstr("too many result addresses: 16, while at most 10 are allowed"));

    /// `remote` has a dedicated setting and no object storage hint.
    const auto remote = message("127.0.0.{1..100}", 10, {});
    EXPECT_THAT(remote, testing::HasSubstr("Table function 'remote'"));
    EXPECT_THAT(remote, testing::HasSubstr("'table_function_remote_max_addresses' setting"));
    EXPECT_THAT(remote, testing::Not(testing::HasSubstr("'s3'")));

    /// The surfaces that share the parser with the table function are named as the user knows them.
    const auto engine = message("a{1..100}", 10, urlCaller("Table engine 'URL'", "'S3' (or another object storage table engine)"));
    EXPECT_THAT(engine, testing::HasSubstr("Table engine 'URL'"));
    EXPECT_THAT(engine, testing::Not(testing::HasSubstr("Table function")));

    const auto mysql = message("host{1..100}:3306", 10, globCaller("Table engine 'MySQL'"));
    EXPECT_THAT(mysql, testing::HasSubstr("Table engine 'MySQL'"));
    EXPECT_THAT(mysql, testing::HasSubstr("'glob_expansion_max_elements' setting"));
    /// Only the `url` family gets the object storage hint.
    EXPECT_THAT(mysql, testing::Not(testing::HasSubstr("'s3'")));
}

TEST(ParseRemoteDescription, TooManyAddressesRecommendsTheMatchingSurface)
{
    /// The hint has to recommend a replacement of the same kind as the surface that was invoked:
    /// a table engine cannot be replaced by a table function, and `urlCluster` needs `s3Cluster`.
    auto message = [](const RemoteDescriptionCaller & caller)
    {
        try
        {
            parseRemoteDescription("a{1..100}", 0, 9, ',', 10, caller);
        }
        catch (const Exception & e)
        {
            return String(e.message());
        }
        return String("no exception");
    };

    const auto table_function = message(urlCaller("Table function 'url'", "'s3' (or another object storage table function)"));
    EXPECT_THAT(table_function, testing::HasSubstr("Use 's3' (or another object storage table function) if"));

    const auto table_engine = message(urlCaller("Table engine 'URL'", "'S3' (or another object storage table engine)"));
    EXPECT_THAT(table_engine, testing::HasSubstr("Use 'S3' (or another object storage table engine) if"));
    /// A table engine must not be told to use a table function.
    EXPECT_THAT(table_engine, testing::Not(testing::HasSubstr("'s3' (")));

    const auto cluster_function
        = message(urlCaller("Table function 'urlCluster'", "'s3Cluster' (or another object storage cluster table function)"));
    EXPECT_THAT(cluster_function, testing::HasSubstr("Use 's3Cluster' (or another object storage cluster table function) if"));
}

TEST(ParseRemoteDescription, TooManyAddressesReportsTheWholeCardinality)
{
    /// The number in the message is what `glob_expansion_max_elements` has to cover, so it has to be
    /// the cardinality of the whole first argument. The parser throws as soon as one factor of the
    /// direct product exceeds the limit, so the count it has at hand ignores the already expanded
    /// prefix and the factors that follow; a separate cardinality pass supplies the real number.
    auto message = [](const String & description, char separator, size_t max_addresses)
    {
        try
        {
            parseRemoteDescription(description, 0, description.size(), separator, max_addresses, {});
        }
        catch (const Exception & e)
        {
            return String(e.message());
        }
        return String("no exception");
    };

    /// The prefix is already expanded when the interval overflows: 2 * 1001, not 1001.
    EXPECT_THAT(message("a{1,2}{1..1001}", ',', 1000), testing::HasSubstr("too many result addresses: 2002,"));

    /// The suffix factors are not looked at when the prefix overflows: 1001 * 2, not 1001.
    EXPECT_THAT(message("{1..1001}a{1,2}", ',', 1000), testing::HasSubstr("too many result addresses: 2002,"));

    /// A direct product of two huge intervals, neither of which is materialized.
    EXPECT_THAT(message("{0..10000}{0..10000}", ',', 1000), testing::HasSubstr("too many result addresses: 100020001,"));

    /// The alternatives are summed, not multiplied.
    EXPECT_THAT(message("{1..600},{1..600}", ',', 1000), testing::HasSubstr("too many result addresses: 1200,"));

    /// When the cardinality does not fit into `size_t` there is no number to report, and the
    /// count-free form of the message is used instead.
    const auto overflowing = message("{0..1000000000000000}{0..1000000000000000}{0..1000000000000000}", ',', 1000);
    EXPECT_THAT(overflowing, testing::HasSubstr("generates too many result addresses, while at most 1000 are allowed"));
}

TEST(ParseRemoteDescription, WithFailoverLimitsTheTotal)
{
    /// Shards and replicas are expanded in two stages, and the limit is on the number of addresses the
    /// whole first argument generates, not on what each stage generates on its own: two shards with
    /// two replicas each are four addresses.
    const auto shards = parseRemoteDescriptionWithFailover("example01-0{1,2}-{1|2}", 4);
    ASSERT_EQ(shards.size(), 2);
    EXPECT_EQ(shards[0].description, "example01-01-{1|2}");
    EXPECT_EQ(shards[0].replicas, (std::vector<String>{"example01-01-1", "example01-01-2"}));
    EXPECT_EQ(shards[1].description, "example01-02-{1|2}");
    EXPECT_EQ(shards[1].replicas, (std::vector<String>{"example01-02-1", "example01-02-2"}));

    EXPECT_THROW(parseRemoteDescriptionWithFailover("example01-0{1,2}-{1|2}", 3), Exception);

    /// A top-level `|` splits every generated shard: four shards with two replicas each.
    EXPECT_EQ(parseRemoteDescriptionWithFailover("h{1..2}|h{3..4}", 8).size(), 4);
    EXPECT_THROW(parseRemoteDescriptionWithFailover("h{1..2}|h{3..4}", 7), Exception);

    /// The replica pattern may differ between the shards.
    const auto uneven = parseRemoteDescriptionWithFailover("a|b,c", 3);
    ASSERT_EQ(uneven.size(), 2);
    EXPECT_EQ(uneven[0].replicas, (std::vector<String>{"a", "b"}));
    EXPECT_EQ(uneven[1].replicas, (std::vector<String>{"c"}));
    EXPECT_THROW(parseRemoteDescriptionWithFailover("a|b,c", 2), Exception);
}

TEST(ParseRemoteDescription, WithFailoverReportsTheWholeCardinality)
{
    /// Whichever of the two stages hits the limit, the reported number covers both of them.
    auto message = [](const String & description, size_t max_addresses)
    {
        try
        {
            parseRemoteDescriptionWithFailover(description, max_addresses, {});
        }
        catch (const Exception & e)
        {
            return String(e.message());
        }
        return String("no exception");
    };

    /// Neither stage exceeds the limit on its own; the total does.
    EXPECT_THAT(message("example01-0{1,2}-{1|2}", 3), testing::HasSubstr("too many result addresses: 4, while at most 3 are allowed"));

    /// The shard stage exceeds the limit before the replicas are looked at: 2000 * 2, not 2000.
    EXPECT_THAT(message("h{1..2000}-{1|2}", 1000), testing::HasSubstr("too many result addresses: 4000, while at most 1000 are allowed"));

    /// The replica stage of one shard exceeds the limit: 2 * 11, not 11.
    EXPECT_THAT(
        message("{a,b}-{1|2|3|4|5|6|7|8|9|10|11}", 10), testing::HasSubstr("too many result addresses: 22, while at most 10 are allowed"));

    /// A top-level `|` splits every generated shard: 2 * 2 shards, 2 replicas each.
    EXPECT_THAT(message("h{1..2}|h{3..4}", 7), testing::HasSubstr("too many result addresses: 8, while at most 7 are allowed"));

    /// The replica pattern may differ between the shards: 2 + 1.
    EXPECT_THAT(message("a|b,c", 2), testing::HasSubstr("too many result addresses: 3, while at most 2 are allowed"));

    /// A single-stage description is still counted as before.
    EXPECT_THAT(message("{1..600},{1..600}", 1000), testing::HasSubstr("too many result addresses: 1200,"));
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
    const auto caller = globCaller("Table engine 'PostgreSQL'");
    EXPECT_EQ(
        parseRemoteDescriptionForExternalDatabase("host1:5432|host2:5433", 10, 5432, caller),
        (Addresses{{"host1", 5432}, {"host2", 5433}}));
    EXPECT_EQ(parseRemoteDescriptionForExternalDatabase("host1", 10, 5432, caller), (Addresses{{"host1", 5432}}));
    EXPECT_EQ(parseRemoteDescriptionForExternalDatabase("[2001:db8::1]:5432", 10, 5432, caller), (Addresses{{"2001:db8::1", 5432}}));
}
