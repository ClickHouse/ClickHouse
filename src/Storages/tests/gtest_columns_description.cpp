#include <Storages/ColumnsDescription.h>
#include <Common/StringUtils.h>
#include <Common/tests/gtest_global_register.h>

#include <Poco/Logger.h>
#include <Poco/AutoPtr.h>
#include <Poco/ConsoleChannel.h>

#include <gtest/gtest.h>

using namespace DB;

class ColumnsDescriptionTest : public testing::Test
{
public:
    void SetUp() override
    {
        Poco::AutoPtr<Poco::ConsoleChannel> channel(new Poco::ConsoleChannel(std::cerr));
        Poco::Logger::root().setChannel(channel);

        if (const char * test_log_level = std::getenv("TEST_LOG_LEVEL")) // NOLINT(concurrency-mt-unsafe)
            Poco::Logger::root().setLevel(test_log_level);
        else
            Poco::Logger::root().setLevel("none");
    }

    void TearDown() override {}
};

TEST_F(ColumnsDescriptionTest, Normalize)
{
    constexpr auto columns = "columns format version: 1\n"
                             "3 columns:\n"
                             "`a` UInt32\n"
                             "`b` String\tDEFAULT\tIf(a = 0, 'true', 'false')\n"
                             "`c` String\tDEFAULT\tcAsT(a, 'String')\n";

    constexpr auto columns_normalized = "columns format version: 1\n"
                                        "3 columns:\n"
                                        "`a` UInt32\n"
                                        "`b` String\tDEFAULT\tif(a = 0, 'true', 'false')\n"
                                        "`c` String\tDEFAULT\tcast(a, 'String')\n";

    tryRegisterFunctions();

    ASSERT_EQ(ColumnsDescription::parse(columns), ColumnsDescription::parse(columns_normalized));
}

TEST_F(ColumnsDescriptionTest, ColumnsSameAsSubcolumns1)
{
    auto columns = ColumnsDescription::parse(trim(R"(
columns format version: 1
3 columns:
`attribute` Map(LowCardinality(String), String)
`attribute.names` Array(LowCardinality(String))	ALIAS mapKeys(attribute)
`attribute.values` Array(String)	ALIAS mapValues(attribute)
    )", isWhitespaceASCII) + "\n");

    for (const auto & column : columns)
    {
        columns.modify(column.name, [&](ColumnDescription &)
        {
            /* No-op */
        });
    }
}

TEST_F(ColumnsDescriptionTest, ColumnsSameAsSubcolumns2)
{
    auto columns = ColumnsDescription::parse(trim(R"(
columns format version: 1
3 columns:
`attribute.names` Array(LowCardinality(String))	ALIAS mapKeys(attribute)
`attribute.values` Array(String)	ALIAS mapValues(attribute)
`attribute` Map(LowCardinality(String), String)
    )", isWhitespaceASCII) + "\n");

    for (const auto & column : columns)
    {
        columns.modify(column.name, [&](ColumnDescription &)
        {
            /* No-op */
        });
    }
}

TEST_F(ColumnsDescriptionTest, StatisticsTypeNameCaseIsNotSignificant)
{
    /// `stringToStatisticsType` lowercases the name, so a case-only difference is the same
    /// statistics type. A replica declaring `STATISTICS(TDigest)` where the stored definition says
    /// `STATISTICS(tdigest)` must still start up instead of failing with `INCOMPATIBLE_COLUMNS`.
    auto parse_one = [](const String & statistics_declaration)
    {
        return ColumnsDescription::parse(
            "columns format version: 1\n1 columns:\n`a` UInt64\tSTATISTICS(" + statistics_declaration + ")\n");
    };

    EXPECT_EQ(parse_one("tdigest"), parse_one("TDigest"));
    EXPECT_EQ(parse_one("tdigest(1)"), parse_one("TDigest(1)"));
    EXPECT_EQ(parse_one("countmin"), parse_one("CountMin"));

    /// A different type, and a different parameter of the same type, are still different.
    EXPECT_NE(parse_one("tdigest"), parse_one("uniq"));
    EXPECT_NE(parse_one("tdigest(1)"), parse_one("tdigest(2)"));

    /// Parameters are ordinary expressions, so the function names in them are canonicalized too.
    EXPECT_EQ(parse_one("tdigest(ABS(-1))"), parse_one("TDigest(abs(-1))"));
    EXPECT_NE(parse_one("tdigest(ABS(-1))"), parse_one("tdigest(ABS(-2))"));
}
