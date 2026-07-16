#include <gtest/gtest.h>
#include <Common/ClickHouseVersion.h>
#include <Common/Exception.h>

using namespace DB;

TEST(ClickHouseVersion, Accepts)
{
    EXPECT_EQ(ClickHouseVersion("26.6").toString(), "26.6");
    EXPECT_EQ(ClickHouseVersion("18.12.17").toString(), "18.12.17");
    EXPECT_EQ(ClickHouseVersion("26.3.1.20001").toString(), "26.3.1.20001");
    EXPECT_EQ(ClickHouseVersion("26.1.3.20001.altinityantalya").toString(), "26.1.3.20001.altinityantalya");
    EXPECT_EQ(ClickHouseVersion("26.1.3.20001.altinitystable").toString(), "26.1.3.20001.altinitystable");
}

TEST(ClickHouseVersion, Rejects)
{
    EXPECT_THROW(ClickHouseVersion(""), Exception);                        /// empty
    EXPECT_THROW(ClickHouseVersion("26"), Exception);                      /// too few components
    EXPECT_THROW(ClickHouseVersion("26.3.1.20001.1"), Exception);          /// too many components
    EXPECT_THROW(ClickHouseVersion("26..1"), Exception);                   /// empty component
    EXPECT_THROW(ClickHouseVersion("26.x"), Exception);                    /// non-numeric component
    EXPECT_THROW(ClickHouseVersion("26.altinityantalya"), Exception);      /// suffix without a full version
    EXPECT_THROW(ClickHouseVersion("26.3.1.20001.altinitycloud"), Exception); /// unknown suffix
}
