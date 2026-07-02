#include <Storages/ColumnsDescription.h>
#include <Common/StringUtils.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>
#include <DataTypes/DataTypesNumber.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTColumnsMatcher.h>
#include <Parsers/ASTIdentifier.h>

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

TEST_F(ColumnsDescriptionTest, ExpandRootColumnMatcherExpression)
{
    ColumnsDescription columns
    {
        ColumnDescription{"a", std::make_shared<DataTypeUInt8>()},
        ColumnDescription{"b", std::make_shared<DataTypeUInt8>()},
    };

    auto matcher = make_intrusive<ASTColumnsRegexpMatcher>();
    matcher->setPattern("^a$");
    ASTPtr expression = matcher;

    expandColumnMatchersInExpression(expression, columns, getContext().context);

    const auto * identifier = expression->as<ASTIdentifier>();
    ASSERT_NE(identifier, nullptr);
    EXPECT_EQ(identifier->name(), "a");
}

TEST_F(ColumnsDescriptionTest, RejectRootColumnMatcherExpressionWithMultipleColumns)
{
    ColumnsDescription columns
    {
        ColumnDescription{"a", std::make_shared<DataTypeUInt8>()},
        ColumnDescription{"b", std::make_shared<DataTypeUInt8>()},
    };

    ASTPtr expression = make_intrusive<ASTAsterisk>();

    EXPECT_THROW(expandColumnMatchersInExpression(expression, columns, getContext().context), Exception);
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
