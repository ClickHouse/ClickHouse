#include <gtest/gtest.h>

#include <Common/Exception.h>
#include <Storages/PostgreSQL/MaterializedPostgreSQLConsumer.h>

namespace DB::ErrorCodes
{
    extern const int POSTGRESQL_REPLICATION_INTERNAL_ERROR;
}

using Consumer = DB::MaterializedPostgreSQLConsumer;

TEST(MaterializedPostgreSQLConsumerReadHelpers, ThrowOnOverrun)
{
    const std::string msg = "0123456789abcdef";
    size_t pos;

    pos = msg.size() - 1;
    EXPECT_THROW(Consumer::readInt8(msg.data(), pos, msg.size()), DB::Exception);
    pos = msg.size() - 3;
    EXPECT_THROW(Consumer::readInt16(msg.data(), pos, msg.size()), DB::Exception);
    pos = msg.size() - 7;
    EXPECT_THROW(Consumer::readInt32(msg.data(), pos, msg.size()), DB::Exception);
    pos = msg.size() - 15;
    EXPECT_THROW(Consumer::readInt64(msg.data(), pos, msg.size()), DB::Exception);

    const std::string not_nul_terminated = "4869";
    pos = 0;
    String result;
    EXPECT_THROW(
        Consumer::readString(not_nul_terminated.data(), pos, not_nul_terminated.size(), result),
        DB::Exception);
}
