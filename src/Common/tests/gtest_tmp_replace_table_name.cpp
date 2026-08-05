#include <gtest/gtest.h>
#include <optional>
#include <Interpreters/TemporaryReplaceTableName.h>

TEST(TemporaryReplaceTableName, FromStringValidInput)
{
    auto result = DB::TemporaryReplaceTableName::fromString("_tmp_replace_efe23F_wef44wE");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->name_hash, "efe23F");
    EXPECT_EQ(result->random_suffix, "wef44wE");
}

TEST(TemporaryReplaceTableName, FromStringMissingRandomSuffix)
{
    auto result = DB::TemporaryReplaceTableName::fromString("_tmp_replace_efe23F_");
    ASSERT_FALSE(result.has_value());
}

TEST(TemporaryReplaceTableName, FromStringMissingNameHash)
{
    auto result = DB::TemporaryReplaceTableName::fromString("_tmp_replace__wef44we");
    ASSERT_FALSE(result.has_value());
}

TEST(TemporaryReplaceTableName, FromStringInvalidPrefix)
{
    auto result = DB::TemporaryReplaceTableName::fromString("wrongprefix_efe23F_wef44we");
    ASSERT_FALSE(result.has_value());
}

TEST(TemporaryReplaceTableName, FromStringNormalTableName)
{
    auto result = DB::TemporaryReplaceTableName::fromString("normal_table_name");
    ASSERT_FALSE(result.has_value());
}

TEST(TemporaryReplaceTableName, ToStringValidInput)
{
    DB::TemporaryReplaceTableName t{.name_hash = "abc123", .random_suffix = "42abc"};
    EXPECT_EQ(t.toString(), "_tmp_replace_abc123_42abc");
}
