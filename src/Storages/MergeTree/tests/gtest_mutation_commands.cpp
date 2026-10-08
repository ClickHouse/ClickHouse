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
    WriteBufferFromOwnString out;
    commands.writeText(out, true);
    EXPECT_EQ(out.str(), "(MODIFY TTL timestamp + toIntervalMonth(3)), (MATERIALIZE TTL)");
}
