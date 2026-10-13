#include <Storages/ColumnsDescription.h>
#include <Storages/StorageInMemoryMetadata.h>
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

TEST_F(ColumnsDescriptionTest, CopySharingASTs)
{
    constexpr auto columns_text = "columns format version: 1\n"
                                  "4 columns:\n"
                                  "`a` UInt32\n"
                                  "`b` UInt64\tDEFAULT\ta + 1\n"
                                  "`c` UInt64\tALIAS\ta * 2\n"
                                  "`d` Date\tCODEC(ZSTD(1))\tTTL d + toIntervalDay(1)\n";

    tryRegisterFunctions();

    const auto columns = ColumnsDescription::parse(columns_text);
    const auto shared = columns.copySharingASTs();
    const ColumnsDescription cloned(columns); /// NOLINT(performance-unnecessary-copy-initialization)

    ASSERT_EQ(shared, columns);
    ASSERT_EQ(cloned, columns);

    for (const auto * name : {"b", "c"})
    {
        const auto * expression = columns.get(name).default_desc.expression.get();
        ASSERT_NE(expression, nullptr);
        ASSERT_EQ(shared.get(name).default_desc.expression.get(), expression);
        ASSERT_NE(cloned.get(name).default_desc.expression.get(), expression);
    }

    const auto * codec = columns.get("d").codec.get();
    const auto * ttl = columns.get("d").ttl.get();
    ASSERT_NE(codec, nullptr);
    ASSERT_NE(ttl, nullptr);
    ASSERT_EQ(shared.get("d").codec.get(), codec);
    ASSERT_EQ(shared.get("d").ttl.get(), ttl);
    ASSERT_NE(cloned.get("d").codec.get(), codec);
    ASSERT_NE(cloned.get("d").ttl.get(), ttl);

    StorageInMemoryMetadata metadata;
    metadata.setColumns(columns);
    const auto shared_metadata = metadata.copySharingColumnASTs();
    const StorageInMemoryMetadata cloned_metadata(metadata); /// NOLINT(performance-unnecessary-copy-initialization)

    const auto * alias = metadata.getColumns().get("c").default_desc.expression.get();
    const auto * metadata_codec = metadata.getColumns().get("d").codec.get();
    ASSERT_NE(alias, nullptr);
    ASSERT_NE(metadata_codec, nullptr);
    ASSERT_EQ(shared_metadata.getColumns().get("c").default_desc.expression.get(), alias);
    ASSERT_EQ(shared_metadata.getColumns().get("d").codec.get(), metadata_codec);
    ASSERT_NE(cloned_metadata.getColumns().get("c").default_desc.expression.get(), alias);
    ASSERT_NE(cloned_metadata.getColumns().get("d").codec.get(), metadata_codec);
}
