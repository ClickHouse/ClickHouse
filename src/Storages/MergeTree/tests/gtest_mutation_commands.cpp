#include <gtest/gtest.h>

#include <Storages/MutationCommands.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>


using namespace DB;

/// This ensures we didn't break compatibility with the serialization format.
TEST(MutationsCommands, Deserialization)
{
    std::string_view str{"MODIFY TTL timestamp + toIntervalMonth(3), MATERIALIZE TTL"};
    ReadBufferFromString in(str);
    MutationCommands commands;
    commands.readText(in, true);
    ASSERT_EQ(commands.size(), 2);
    EXPECT_FALSE(commands[0].targets_exact_column);
    EXPECT_FALSE(commands[1].targets_exact_column);
    WriteBufferFromOwnString out;
    commands.writeText(out, true);
    EXPECT_EQ(out.str(), "(MODIFY TTL timestamp + toIntervalMonth(3)), (MATERIALIZE TTL)");
}

TEST(MutationsCommands, UnparenthesizedClearColumn)
{
    std::string_view str{"CLEAR COLUMN n"};
    ReadBufferFromString in(str);
    MutationCommands commands;
    commands.readText(in, false);
    ASSERT_EQ(commands.size(), 1);
    EXPECT_EQ(commands[0].column_name, "n");
    EXPECT_TRUE(commands[0].clear);
    EXPECT_FALSE(commands[0].targets_exact_column);
}
