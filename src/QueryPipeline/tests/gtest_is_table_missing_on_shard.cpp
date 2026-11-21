#include <gtest/gtest.h>

#include <Interpreters/StorageID.h>
#include <QueryPipeline/UnavailableShardTracker.h>
#include <Common/Exception.h>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int UNKNOWN_IDENTIFIER;
    extern const int UNKNOWN_TABLE;
}

namespace
{

bool isMissing(const String & message, const StorageID & table = {"db", "t"}, const String & default_database = "", int code = ErrorCodes::UNKNOWN_TABLE)
{
    return isTableMissingOnShard(Exception::createRuntime(code, "Received from host:9000. DB::Exception: " + message), table, default_database);
}

}

TEST(IsTableMissingOnShard, Messages)
{
    for (const auto * message : {
             "Table db.t does not exist.",
             "Table db.t does not exist. Maybe you meant db.t2?. Stack trace:\n\n0. ...",
             "Table db.t does not exist: While processing x.",
             "Table db.t (6e2a3b1c-0000-4000-8000-000000000001) does not exist.",
             "Table 'db.t (6e2a3b1c-0000-4000-8000-000000000001)' doesn't exists..",
             "Table 'db.t' doesn't exists..",
             "Database db does not exist.",
             "Database db does not exist. Maybe you meant db2?.",
             "Unknown table expression identifier 'db.t'. In scope SELECT 1 FROM db.t.",
             "Unknown table expression identifier 'db.t'. Maybe you meant db.t2? In scope SELECT 1 FROM db.t.",
             "Unknown table expression identifier 'db.t' in scope SELECT 1 FROM db.t.", /// Before 26.10
             "Unknown table db.t.",
             "Unknown table db.t. Stack trace:\n\n0. ...",
         })
        EXPECT_TRUE(isMissing(message)) << message;

    for (const auto * message : {
             "Table db.t2 does not exist. Maybe you meant db.t?.",
             "Table db2.t does not exist.",
             "Table db.`.inner_id.6e2a3b1c-0000-4000-8000-000000000001` does not exist.",
             "Target table 'db.t' of view 'db.t2' doesn't exists..",
             "Database db2 does not exist. Maybe you meant db?.",
             "Unknown table expression identifier 'db.t2'. In scope SELECT 1 FROM db.t, db.t2.",
             "Unknown table expression identifier 'db.t'2'. In scope SELECT 1 FROM db.`t'2`.",
             "Unknown table expression identifier 't'. In scope SELECT 1 FROM t.",
             "Unknown table db.t. 2.",
             "Received from another:9000. DB::Exception: Table db.t does not exist.",
         })
        EXPECT_FALSE(isMissing(message)) << message;

    EXPECT_FALSE(isMissing("Table db.t does not exist.", {"db", "t"}, "", ErrorCodes::UNKNOWN_IDENTIFIER));
}

TEST(IsTableMissingOnShard, Names)
{
    EXPECT_TRUE(isMissing("Table db.values does not exist.", {"db", "values"}));
    EXPECT_TRUE(isMissing("Table db.`values` does not exist.", {"db", "values"}));
    EXPECT_TRUE(isMissing("Table `d(b`.`t\\`` does not exist.", {"d(b", "t`"}));
    EXPECT_TRUE(isMissing("Unknown table expression identifier 'd(b.t`'. In scope x.", {"d(b", "t`"}));

    EXPECT_TRUE(isMissing("Table a.`b.c` does not exist.", {"a", "b.c"}));
    EXPECT_FALSE(isMissing("Table `a.b`.c does not exist.", {"a", "b.c"}));
    EXPECT_FALSE(isMissing("Unknown table expression identifier 'a.b.c'. In scope x.", {"a", "b.c"}));
    EXPECT_FALSE(isMissing("Unknown table a.b.c.", {"a.b", "c"}));
}

TEST(IsTableMissingOnShard, DefaultDatabase)
{
    EXPECT_TRUE(isMissing("Unknown table expression identifier 't'. In scope x.", {"", "t"}, "db"));
    EXPECT_TRUE(isMissing("Table db.t does not exist.", {"", "t"}, "db"));
    EXPECT_TRUE(isMissing("Database db does not exist.", {"", "t"}, "db"));
    EXPECT_FALSE(isMissing("Table db2.t does not exist.", {"", "t"}, "db"));
    EXPECT_FALSE(isMissing("Database db2 does not exist.", {"db", "t"}, "db2"));

    EXPECT_TRUE(isMissing("Table db2.t does not exist.", {"", "t"}));
    EXPECT_FALSE(isMissing("Database db2 does not exist.", {"", "t"}));
}
