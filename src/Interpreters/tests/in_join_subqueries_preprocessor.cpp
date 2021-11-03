#include <ext/shared_ptr_helper.h>

#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>
#include <Interpreters/InJoinSubqueriesPreprocessor.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>
#include <Storages/IStorage.h>
#include <Databases/IDatabase.h>
#include <Databases/DatabaseOrdinary.h>
#include <Common/typeid_cast.h>

#include <iostream>
#include <vector>
#include <utility>
#include <string>


namespace DB
{
    namespace ErrorCodes
    {
        extern const int DISTRIBUTED_IN_JOIN_SUBQUERY_DENIED;
    }
}


/// Simplified version of the StorageDistributed class.
class StorageDistributedFake final : public ext::shared_ptr_helper<StorageDistributedFake>, public DB::IStorage
{
    friend struct ext::shared_ptr_helper<StorageDistributedFake>;
public:
    std::string getName() const override { return "DistributedFake"; }
    bool isRemote() const override { return true; }
    size_t getShardCount() const { return shard_count; }
    std::string getRemoteDatabaseName() const { return remote_database; }
    std::string getRemoteTableName() const { return remote_table; }

protected:
    StorageDistributedFake(const std::string & remote_database_, const std::string & remote_table_, size_t shard_count_)
        : IStorage({"", ""}), remote_database(remote_database_), remote_table(remote_table_), shard_count(shard_count_)
    {
    }

private:
    const std::string remote_database;
    const std::string remote_table;
    size_t shard_count;
};


class CheckShardsAndTablesMock : public DB::InJoinSubqueriesPreprocessor::CheckShardsAndTables
{
public:
    bool hasAtLeastTwoShards(const DB::IStorage & table) const override
    {
        if (!table.isRemote())
            return false;

        const StorageDistributedFake * distributed = dynamic_cast<const StorageDistributedFake *>(&table);
        if (!distributed)
            return false;

        return distributed->getShardCount() >= 2;
    }

    std::pair<std::string, std::string>
    getRemoteDatabaseAndTableName(const DB::IStorage & table) const override
    {
        const StorageDistributedFake & distributed = dynamic_cast<const StorageDistributedFake &>(table);
        return { distributed.getRemoteDatabaseName(), distributed.getRemoteTableName() };
    }
};


struct TestEntry
{
    unsigned int line_num;
    std::string input;
    std::string expected_output;
    size_t shard_count;
    DB::DistributedProductMode mode;
    bool expected_success;
};

using TestEntries = std::vector<TestEntry>;
using TestResult = std::pair<bool, std::string>;

TestResult check(const TestEntry & entry);
bool parse(DB::ASTPtr & ast, const std::string & query);
bool equals(const DB::ASTPtr & lhs, const DB::ASTPtr & rhs);
void reorder(DB::IAST * ast);


TestEntries entries =
{
    /// Trivial query.

    {
        __LINE__,
        "SELECT 1",
        "SELECT 1",
        0,
        DB::DistributedProductMode::ALLOW,
        true
    },

    {
        __LINE__,
        "SELECT 1",
        "SELECT 1",
        1,
        DB::DistributedProductMode::ALLOW,
        true
    },

    {
        __LINE__,
        "SELECT 1",
        "SELECT 1",
        2,
        DB::DistributedProductMode::ALLOW,
        true
    },

    {
        __LINE__,
        "SELECT 1",
        "SELECT 1",
        0,
        DB::DistributedProductMode::DENY,
        true
    },

    {
        __LINE__,
        "SELECT 1",
        "SELECT 1",
        1,
        DB::DistributedProductMode::DENY,
        true
    },

    {
        __LINE__,
        "SELECT 1",
        "SELECT 1",
        2,
        DB::DistributedProductMode::DENY,
        true
    },

    {
        __LINE__,
        "SELECT 1",
        "SELECT 1",
        0,
        DB::DistributedProductMode::LOCAL,
        true
    },

    {
        __LINE__,
        "SELECT 1",
        "SELECT 1",
        1,
        DB::DistributedProductMode::LOCAL,
        true
    },

    {
        __LINE__,
        "SELECT 1",
        "SELECT 1",
        2,
        DB::DistributedProductMode::LOCAL,
        true
    },

    {
        __LINE__,
        "SELECT 1",
        "SELECT 1",
        0,
        DB::DistributedProductMode::GLOBAL,
        true
    },

    {
        __LINE__,
        "SELECT 1",
        "SELECT 1",
        1,
        DB::DistributedProductMode::GLOBAL,
        true
    },

    {
        __LINE__,
        "SELECT 1",
        "SELECT 1",
        2,
        DB::DistributedProductMode::GLOBAL,
        true
    },

    /// Section IN / depth 1

    {
        __LINE__,
        "SELECT count() FROM test.visits_all WHERE UserID IN (SELECT UserID FROM test.visits_all)",
        "SELECT count() FROM test.visits_all WHERE UserID IN (SELECT UserID FROM test.visits_all)",
        1,
        DB::DistributedProductMode::ALLOW,
        true
    },

    {
        __LINE__,
        "SELECT count() FROM test.visits_all WHERE UserID IN (SELECT UserID FROM test.visits_all)",
        "SELECT count() FROM test.visits_all WHERE UserID IN (SELECT UserID FROM test.visits_all)",
        2,
        DB::DistributedProductMode::ALLOW,
        true
    },

    {
        __LINE__,
        "SELECT count() FROM test.visits_all WHERE UserID IN (SELECT UserID FROM test.visits_all)",
        "SELECT count() FROM test.visits_all WHERE UserID IN (SELECT UserID FROM test.visits_all)",
        1,
        DB::DistributedProductMode::DENY,
        true
    },

    {
        __LINE__,
        "SELECT count() FROM test.visits_all WHERE UserID IN (SELECT UserID FROM test.visits_all)",
        "SELECT count() FROM test.visits_all WHERE UserID IN (SELECT UserID FROM test.visits_all)",
        2,
        DB::DistributedProductMode::DENY,
        false
    },

    {
        __LINE__,
        "SELECT count() FROM test.visits_all WHERE UserID IN (SELECT UserID FROM test.visits_all)",
        "SELECT count() FROM test.visits_all WHERE UserID GLOBAL IN (SELECT UserID FROM test.visits_all)",
        2,
        DB::DistributedProductMode::GLOBAL,
        true
    },

    {
        __LINE__,
        "SELECT count() FROM test.visits_all WHERE UserID IN (SELECT UserID FROM remote_db.remote_visits)",
        "SELECT count() FROM test.visits_all WHERE UserID IN (SELECT UserID FROM remote_db.remote_visits)",
        2,
        DB::DistributedProductMode::GLOBAL,
        true
    },

    {
        __LINE__,
        "SELECT count() FROM test.visits_all WHERE UserID IN (SELECT UserID FROM test.visits_all)",
        "SELECT count() FROM test.visits_all WHERE UserID IN (SELECT UserID FROM remote_db.remote_visits)",
        2,
        DB::DistributedProductMode::LOCAL,
        true
    },

    {
        __LINE__,
        "SELECT count() FROM test.visits_all WHERE UserID IN (SELECT UserID FROM remote_db.remote_visits)",
        "SELECT count() FROM test.visits_all WHERE UserID IN (SELECT UserID FROM remote_db.remote_visits)",
        2,
        DB::DistributedProductMode::LOCAL,
        true
    },

    /// Section NOT IN / depth 1

    {
        __LINE__,
        "SELECT count() FROM test.visits_all WHERE UserID NOT IN (SELECT UserID FROM test.visits_all)",
        "SELECT count() FROM test.visits_all WHERE UserID NOT IN (SELECT UserID FROM test.visits_all)",
        2,
        DB::DistributedProductMode::ALLOW,
        true
    },

    {
        __LINE__,
        "SELECT count() FROM test.visits_all WHERE UserID NOT IN (SELECT UserID FROM test.visits_all)",
        "SELECT count() FROM test.visits_all WHERE UserID NOT IN (SELECT UserID FROM test.visits_all)",
        2,
        DB::DistributedProductMode::DENY,
        false
    },

    {
        __LINE__,
        "SELECT count() FROM test.visits_all WHERE UserID NOT IN (SELECT UserID FROM test.visits_all)",
        "SELECT count() FROM test.visits_all WHERE UserID GLOBAL NOT IN (SELECT UserID FROM test.visits_all)",
        2,
        DB::DistributedProductMode::GLOBAL,
        true
    },

    {
        __LINE__,
        "SELECT count() FROM test.visits_all WHERE UserID NOT IN (SELECT UserID FROM remote_db.remote_visits)",
        "SELECT count() FROM test.visits_all WHERE UserID NOT IN (SELECT UserID FROM remote_db.remote_visits)",
        2,
        DB::DistributedProductMode::GLOBAL,
        true
    },

    {
        __LINE__,
        "SELECT count() FROM test.visits_all WHERE UserID NOT IN (SELECT UserID FROM test.visits_all)",
        "SELECT count() FROM test.visits_all WHERE UserID NOT IN (SELECT UserID FROM remote_db.remote_visits)",
        2,
        DB::DistributedProductMode::LOCAL,
        true
    },

    {
        __LINE__,
        "SELECT count() FROM test.visits_all WHERE UserID NOT IN (SELECT UserID FROM remote_db.remote_visits)",
        "SELECT count() FROM test.visits_all WHERE UserID NOT IN (SELECT UserID FROM remote_db.remote_visits)",
        2,
        DB::DistributedProductMode::LOCAL,
        true
    },

    /// Section GLOBAL IN / depth 1

    {
        __LINE__,
        "SELECT count() FROM test.visits_all WHERE UserID GLOBAL IN (SELECT UserID FROM test.visits_all)",
        "SELECT count() FROM test.visits_all WHERE UserID GLOBAL IN (SELECT UserID FROM test.visits_all)",
        2,
        DB::DistributedProductMode::ALLOW,
        true
    },

    {
        __LINE__,
        "SELECT count() FROM test.visits_all WHERE UserID GLOBAL IN (SELECT UserID FROM test.visits_all)",
        "SELECT count() FROM test.visits_all WHERE UserID GLOBAL IN (SELECT UserID FROM test.visits_all)",
        2,
        DB::DistributedProductMode::DENY,
        true
    },

    {
        __LINE__,
        "SELECT count() FROM test.visits_all WHERE UserID GLOBAL IN (SELECT UserID FROM test.visits_all)",
        "SELECT count() FROM test.visits_all WHERE UserID GLOBAL IN (SELECT UserID FROM test.visits_all)",
        2,
        DB::DistributedProductMode::GLOBAL,
        true
    },

    {
        __LINE__,
        "SELECT count() FROM test.visits_all WHERE UserID GLOBAL IN (SELECT UserID FROM test.visits_all)",
        "SELECT count() FROM test.visits_all WHERE UserID GLOBAL IN (SELECT UserID FROM test.visits_all)",
        2,
        DB::DistributedProductMode::LOCAL,
        true
    },

    /// Section GLOBAL NOT IN / depth 1

    {
        __LINE__,
        "SELECT count() FROM test.visits_all WHERE UserID GLOBAL NOT IN (SELECT UserID FROM test.visits_all)",
        "SELECT count() FROM test.visits_all WHERE UserID GLOBAL NOT IN (SELECT UserID FROM test.visits_all)",
        2,
        DB::DistributedProductMode::ALLOW,
        true
    },

    {
        __LINE__,
        "SELECT count() FROM test.visits_all WHERE UserID GLOBAL NOT IN (SELECT UserID FROM test.visits_all)",
        "SELECT count() FROM test.visits_all WHERE UserID GLOBAL NOT IN (SELECT UserID FROM test.visits_all)",
        2,
        DB::DistributedProductMode::DENY,
        true
    },

    {
        __LINE__,
        "SELECT count() FROM test.visits_all WHERE UserID GLOBAL NOT IN (SELECT UserID FROM test.visits_all)",
        "SELECT count() FROM test.visits_all WHERE UserID GLOBAL NOT IN (SELECT UserID FROM test.visits_all)",
        2,
        DB::DistributedProductMode::GLOBAL,
        true
    },

    {
        __LINE__,
        "SELECT count() FROM test.visits_all WHERE UserID GLOBAL NOT IN (SELECT UserID FROM test.visits_all)",
        "SELECT count() FROM test.visits_all WHERE UserID GLOBAL NOT IN (SELECT UserID FROM test.visits_all)",
        2,
        DB::DistributedProductMode::LOCAL,
        true
    },

    /// Section JOIN / depth 1

    {
        __LINE__,
        "SELECT UserID FROM test.visits_all ALL INNER JOIN (SELECT UserID FROM test.visits_all) USING UserID",
        "SELECT UserID FROM test.visits_all ALL INNER JOIN (SELECT UserID FROM test.visits_all) USING UserID",
        2,
        DB::DistributedProductMode::ALLOW,
        true
    },

    {
        __LINE__,
        "SELECT UserID FROM test.visits_all ALL INNER JOIN (SELECT UserID FROM test.visits_all) USING UserID",
        "SELECT UserID FROM test.visits_all ALL INNER JOIN (SELECT UserID FROM test.visits_all) USING UserID",
        2,
        DB::DistributedProductMode::DENY,
        false
    },

    {
        __LINE__,
        "SELECT UserID FROM test.visits_all ALL INNER JOIN (SELECT UserID FROM test.visits_all) USING UserID",
        "SELECT UserID FROM test.visits_all GLOBAL ALL INNER JOIN (SELECT UserID FROM test.visits_all) USING UserID",
        2,
        DB::DistributedProductMode::GLOBAL,
        true
    },

    {
        __LINE__,
        "SELECT UserID FROM test.visits_all ALL INNER JOIN (SELECT UserID FROM remote_db.remote_visits) USING UserID",
        "SELECT UserID FROM test.visits_all ALL INNER JOIN (SELECT UserID FROM remote_db.remote_visits) USING UserID",
        2,
        DB::DistributedProductMode::GLOBAL,
        true
    },

    {
        __LINE__,
        "SELECT UserID FROM test.visits_all ALL INNER JOIN (SELECT UserID FROM test.visits_all) USING UserID",
        "SELECT UserID FROM test.visits_all ALL INNER JOIN (SELECT UserID FROM remote_db.remote_visits) USING UserID",
        2,
        DB::DistributedProductMode::LOCAL,
        true
    },

    {
        __LINE__,
        "SELECT UserID FROM test.visits_all ALL INNER JOIN (SELECT UserID FROM remote_db.remote_visits) USING UserID",
        "SELECT UserID FROM test.visits_all ALL INNER JOIN (SELECT UserID FROM remote_db.remote_visits) USING UserID",
        2,
        DB::DistributedProductMode::LOCAL,
        true
    },

    /// Section GLOBAL JOIN / depth 1

    {
        __LINE__,
        "SELECT UserID FROM test.visits_all GLOBAL ALL INNER JOIN (SELECT UserID FROM test.visits_all) USING UserID",
        "SELECT UserID FROM test.visits_all GLOBAL ALL INNER JOIN (SELECT UserID FROM test.visits_all) USING UserID",
        2,
        DB::DistributedProductMode::ALLOW,
        true
    },

    {
        __LINE__,
        "SELECT UserID FROM test.visits_all GLOBAL ALL INNER JOIN (SELECT UserID FROM test.visits_all) USING UserID",
        "SELECT UserID FROM test.visits_all GLOBAL ALL INNER JOIN (SELECT UserID FROM test.visits_all) USING UserID",
        2,
        DB::DistributedProductMode::DENY,
        true
    },

    {
        __LINE__,
        "SELECT UserID FROM test.visits_all GLOBAL ALL INNER JOIN (SELECT UserID FROM test.visits_all) USING UserID",
        "SELECT UserID FROM test.visits_all GLOBAL ALL INNER JOIN (SELECT UserID FROM test.visits_all) USING UserID",
        2,
        DB::DistributedProductMode::GLOBAL,
        true
    },

    {
        __LINE__,
        "SELECT UserID FROM test.visits_all GLOBAL ALL INNER JOIN (SELECT UserID FROM test.visits_all) USING UserID",
        "SELECT UserID FROM test.visits_all GLOBAL ALL INNER JOIN (SELECT UserID FROM test.visits_all) USING UserID",
        2,
        DB::DistributedProductMode::LOCAL,
        true
    },

    /// Section JOIN / depth 1 / 2 of the subquery.

    {
        __LINE__,
        "SELECT UserID FROM (SELECT UserID FROM test.visits_all WHERE RegionID = 1) ALL INNER JOIN (SELECT UserID FROM test.visits_all WHERE RegionID = 2) USING UserID",
        "SELECT UserID FROM (SELECT UserID FROM test.visits_all WHERE RegionID = 1) ALL INNER JOIN (SELECT UserID FROM test.visits_all WHERE RegionID = 2) USING UserID",
        2,
        DB::DistributedProductMode::ALLOW,
        true
    },

    {
        __LINE__,
        "SELECT UserID FROM (SELECT UserID FROM test.visits_all WHERE RegionID = 1) ALL INNER JOIN (SELECT UserID FROM remote_db.remote_visits WHERE RegionID = 2) USING UserID",
        "SELECT UserID FROM (SELECT UserID FROM test.visits_all WHERE RegionID = 1) ALL INNER JOIN (SELECT UserID FROM remote_db.remote_visits WHERE RegionID = 2) USING UserID",
        2,
        DB::DistributedProductMode::DENY,
        true
    },

    {
        __LINE__,
        "SELECT UserID FROM (SELECT UserID FROM test.visits_all WHERE RegionID = 1) ALL INNER JOIN (SELECT UserID FROM test.visits_all WHERE RegionID = 2) USING UserID",
        "SELECT UserID FROM (SELECT UserID FROM test.visits_all WHERE RegionID = 1) ALL INNER JOIN (SELECT UserID FROM test.visits_all WHERE RegionID = 2) USING UserID",
        2,
        DB::DistributedProductMode::DENY,
        true
    },

    {
        __LINE__,
        "SELECT UserID FROM (SELECT UserID FROM test.visits_all WHERE RegionID = 1) ALL INNER JOIN (SELECT UserID FROM test.visits_all WHERE RegionID = 2) USING UserID",
        "SELECT UserID FROM (SELECT UserID FROM test.visits_all WHERE RegionID = 1) ALL INNER JOIN (SELECT UserID FROM test.visits_all WHERE RegionID = 2) USING UserID",
        2,
        DB::DistributedProductMode::GLOBAL,
        true
    },

    {
        __LINE__,
        "SELECT UserID FROM (SELECT UserID FROM test.visits_all WHERE RegionID = 1) ALL INNER JOIN (SELECT UserID FROM test.visits_all WHERE RegionID = 2) USING UserID",
        "SELECT UserID FROM (SELECT UserID FROM test.visits_all WHERE RegionID = 1) ALL INNER JOIN (SELECT UserID FROM test.visits_all WHERE RegionID = 2) USING UserID",
        2,
        DB::DistributedProductMode::LOCAL,
        true
    },

    /// Section IN / depth 1 / table at level 2

    {
        __LINE__,
        "SELECT count() FROM test.visits_all WHERE UserID IN (SELECT UserID FROM (SELECT UserID FROM test.visits_all))",
        "SELECT count() FROM test.visits_all WHERE UserID IN (SELECT UserID FROM (SELECT UserID FROM test.visits_all))",
        2,
        DB::DistributedProductMode::ALLOW,
        true
    },

    {
        __LINE__,
        "SELECT count() FROM test.visits_all WHERE UserID IN (SELECT UserID FROM (SELECT UserID FROM test.visits_all))",
        "SELECT count() FROM test.visits_all WHERE UserID IN (SELECT UserID FROM (SELECT UserID FROM test.visits_all))",
        2,
        DB::DistributedProductMode::DENY,
        false
    },

    {
        __LINE__,
        "SELECT count() FROM test.visits_all WHERE UserID IN (SELECT UserID FROM (SELECT UserID FROM test.visits_all))",
        "SELECT count() FROM test.visits_all WHERE UserID GLOBAL IN (SELECT UserID FROM (SELECT UserID FROM test.visits_all))",
        2,
        DB::DistributedProductMode::GLOBAL,
        true
    },

    {
        __LINE__,
        "SELECT count() FROM test.visits_all WHERE UserID IN (SELECT UserID FROM (SELECT UserID FROM remote_db.remote_visits))",
        "SELECT count() FROM test.visits_all WHERE UserID IN (SELECT UserID FROM (SELECT UserID FROM remote_db.remote_visits))",
        2,
        DB::DistributedProductMode::GLOBAL,
        true
    },

    {
        __LINE__,
        "SELECT count() FROM test.visits_all WHERE UserID IN (SELECT UserID FROM (SELECT UserID FROM test.visits_all))",
        "SELECT count() FROM test.visits_all WHERE UserID IN (SELECT UserID FROM (SELECT UserID FROM remote_db.remote_visits))",
        2,
        DB::DistributedProductMode::LOCAL,
        true
    },

    {
        __LINE__,
        "SELECT count() FROM test.visits_all WHERE UserID IN (SELECT UserID FROM (SELECT UserID FROM remote_db.remote_visits))",
        "SELECT count() FROM test.visits_all WHERE UserID IN (SELECT UserID FROM (SELECT UserID FROM remote_db.remote_visits))",
        2,
        DB::DistributedProductMode::LOCAL,
        true
    },

    /// Section GLOBAL IN / depth 1 / table at level 2

    {
        __LINE__,
        "SELECT UserID FROM test.visits_all WHERE UserID GLOBAL IN (SELECT UserID FROM (SELECT UserID FROM test.visits_all))",
        "SELECT UserID FROM test.visits_all WHERE UserID GLOBAL IN (SELECT UserID FROM (SELECT UserID FROM test.visits_all))",
        2,
        DB::DistributedProductMode::ALLOW,
        true
    },

    {
        __LINE__,
        "SELECT UserID FROM test.visits_all WHERE UserID GLOBAL IN (SELECT UserID FROM (SELECT UserID FROM test.visits_all))",
        "SELECT UserID FROM test.visits_all WHERE UserID GLOBAL IN (SELECT UserID FROM (SELECT UserID FROM test.visits_all))",
        2,
        DB::DistributedProductMode::DENY,
        true
    },

    {
        __LINE__,
        "SELECT UserID FROM test.visits_all WHERE UserID GLOBAL IN (SELECT UserID FROM (SELECT UserID FROM test.visits_all))",
        "SELECT UserID FROM test.visits_all WHERE UserID GLOBAL IN (SELECT UserID FROM (SELECT UserID FROM test.visits_all))",
        2,
        DB::DistributedProductMode::GLOBAL,
        true
    },

    {
        __LINE__,
        "SELECT UserID FROM test.visits_all WHERE UserID GLOBAL IN (SELECT UserID FROM (SELECT UserID FROM test.visits_all))",
        "SELECT UserID FROM test.visits_all WHERE UserID GLOBAL IN (SELECT UserID FROM (SELECT UserID FROM test.visits_all))",
        2,
        DB::DistributedProductMode::LOCAL,
        true
    },

    /// Section IN at level 1, GLOBAL IN section at level 2.

    {
        __LINE__,
        "SELECT UserID FROM test.visits_all WHERE UserID IN (SELECT UserID FROM remote_db.remote_visits WHERE UserID GLOBAL IN (SELECT UserID FROM (SELECT UserID FROM test.visits_all)))",
        "SELECT UserID FROM test.visits_all WHERE UserID IN (SELECT UserID FROM remote_db.remote_visits WHERE UserID GLOBAL IN (SELECT UserID FROM (SELECT UserID FROM test.visits_all)))",
        2,
        DB::DistributedProductMode::ALLOW,
        true
    },

    {
        __LINE__,
        "SELECT UserID FROM test.visits_all WHERE UserID IN (SELECT UserID FROM remote_db.remote_visits WHERE UserID GLOBAL IN (SELECT UserID FROM (SELECT UserID FROM test.visits_all)))",
        "SELECT UserID FROM test.visits_all WHERE UserID IN (SELECT UserID FROM remote_db.remote_visits WHERE UserID GLOBAL IN (SELECT UserID FROM (SELECT UserID FROM test.visits_all)))",
        2,
        DB::DistributedProductMode::DENY,
        false
    },

    {
        __LINE__,
        "SELECT UserID FROM test.visits_all WHERE UserID IN (SELECT UserID FROM remote_db.remote_visits WHERE UserID GLOBAL IN (SELECT UserID FROM (SELECT UserID FROM test.visits_all)))",
        "SELECT UserID FROM test.visits_all WHERE UserID IN (SELECT UserID FROM remote_db.remote_visits WHERE UserID GLOBAL IN (SELECT UserID FROM (SELECT UserID FROM remote_db.remote_visits)))",
        2,
        DB::DistributedProductMode::LOCAL,
        true
    },

    /// Section JOIN / depth 1 / table at level 2

    {
        __LINE__,
        "SELECT UserID FROM test.visits_all ALL INNER JOIN (SELECT UserID FROM (SELECT UserID FROM test.visits_all)) USING UserID",
        "SELECT UserID FROM test.visits_all ALL INNER JOIN (SELECT UserID FROM (SELECT UserID FROM test.visits_all)) USING UserID",
        2,
        DB::DistributedProductMode::ALLOW,
        true
    },

    {
        __LINE__,
        "SELECT UserID FROM test.visits_all ALL INNER JOIN (SELECT UserID FROM (SELECT UserID FROM test.visits_all)) USING UserID",
        "SELECT UserID FROM test.visits_all ALL INNER JOIN (SELECT UserID FROM (SELECT UserID FROM test.visits_all)) USING UserID",
        2,
        DB::DistributedProductMode::DENY,
        false
    },

    {
        __LINE__,
        "SELECT UserID FROM test.visits_all ALL INNER JOIN (SELECT UserID FROM (SELECT UserID FROM test.visits_all)) USING UserID",
        "SELECT UserID FROM test.visits_all GLOBAL ALL INNER JOIN (SELECT UserID FROM (SELECT UserID FROM test.visits_all)) USING UserID",
        2,
        DB::DistributedProductMode::GLOBAL,
        true
    },

    {
        __LINE__,
        "SELECT UserID FROM test.visits_all ALL INNER JOIN (SELECT UserID FROM (SELECT UserID FROM remote_db.remote_visits)) USING UserID",
        "SELECT UserID FROM test.visits_all ALL INNER JOIN (SELECT UserID FROM (SELECT UserID FROM remote_db.remote_visits)) USING UserID",
        2,
        DB::DistributedProductMode::GLOBAL,
        true
    },

    {
        __LINE__,
        "SELECT UserID FROM test.visits_all ALL INNER JOIN (SELECT UserID FROM (SELECT UserID FROM test.visits_all)) USING UserID",
        "SELECT UserID FROM test.visits_all ALL INNER JOIN (SELECT UserID FROM (SELECT UserID FROM remote_db.remote_visits)) USING UserID",
        2,
        DB::DistributedProductMode::LOCAL,
        true
    },

    {
        __LINE__,
        "SELECT UserID FROM test.visits_all ALL INNER JOIN (SELECT UserID FROM (SELECT UserID FROM remote_db.remote_visits)) USING UserID",
        "SELECT UserID FROM test.visits_all ALL INNER JOIN (SELECT UserID FROM (SELECT UserID FROM remote_db.remote_visits)) USING UserID",
        2,
        DB::DistributedProductMode::LOCAL,
        true
    },

    /// Section IN / depth 2

    {
        __LINE__,
        "SELECT UserID, RegionID FROM test.visits_all WHERE CounterID IN (SELECT CounterID FROM test.visits_all WHERE BrowserID IN (SELECT BrowserID FROM test.visits_all WHERE OtherID = 1))",
        "SELECT UserID, RegionID FROM test.visits_all WHERE CounterID IN (SELECT CounterID FROM test.visits_all WHERE BrowserID IN (SELECT BrowserID FROM test.visits_all WHERE OtherID = 1))",
        2,
        DB::DistributedProductMode::ALLOW,
        true
    },

    {
        __LINE__,
        "SELECT UserID, RegionID FROM test.visits_all WHERE CounterID GLOBAL IN (SELECT CounterID FROM test.visits_all WHERE BrowserID IN (SELECT BrowserID FROM remote_db.remote_visits WHERE OtherID = 1))",
        "SELECT UserID, RegionID FROM test.visits_all WHERE CounterID GLOBAL IN (SELECT CounterID FROM test.visits_all WHERE BrowserID IN (SELECT BrowserID FROM remote_db.remote_visits WHERE OtherID = 1))",
        2,
        DB::DistributedProductMode::DENY,
        true
    },

    {
        __LINE__,
        "SELECT UserID, RegionID FROM test.visits_all WHERE CounterID IN (SELECT CounterID FROM test.visits_all WHERE BrowserID IN (SELECT BrowserID FROM test.visits_all WHERE OtherID = 1))",
        "SELECT UserID, RegionID FROM test.visits_all WHERE CounterID IN (SELECT CounterID FROM test.visits_all WHERE BrowserID IN (SELECT BrowserID FROM test.visits_all WHERE OtherID = 1))",
        2,
        DB::DistributedProductMode::DENY,
        false
    },

    {
        __LINE__,
        "SELECT UserID, RegionID FROM test.visits_all WHERE CounterID GLOBAL IN (SELECT CounterID FROM test.visits_all WHERE BrowserID IN (SELECT BrowserID FROM test.visits_all WHERE OtherID = 1))",
        "SELECT UserID, RegionID FROM test.visits_all WHERE CounterID GLOBAL IN (SELECT CounterID FROM test.visits_all WHERE BrowserID IN (SELECT BrowserID FROM test.visits_all WHERE OtherID = 1))",
        2,
        DB::DistributedProductMode::DENY,
        true
    },

    {
        __LINE__,
        "SELECT UserID, RegionID FROM test.visits_all WHERE CounterID IN (SELECT CounterID FROM test.visits_all WHERE BrowserID IN (SELECT BrowserID FROM remote_db.remote_visits WHERE OtherID = 1))",
        "SELECT UserID, RegionID FROM test.visits_all WHERE CounterID GLOBAL IN (SELECT CounterID FROM test.visits_all WHERE BrowserID IN (SELECT BrowserID FROM remote_db.remote_visits WHERE OtherID = 1))",
        2,
        DB::DistributedProductMode::GLOBAL,
        true
    },

    {
        __LINE__,
        "SELECT UserID, RegionID FROM test.visits_all WHERE CounterID IN (SELECT CounterID FROM test.visits_all WHERE BrowserID IN (SELECT BrowserID FROM test.visits_all WHERE OtherID = 1))",
        "SELECT UserID, RegionID FROM test.visits_all WHERE CounterID IN (SELECT CounterID FROM remote_db.remote_visits WHERE BrowserID IN (SELECT BrowserID FROM remote_db.remote_visits WHERE OtherID = 1))",
        2,
        DB::DistributedProductMode::LOCAL,
        true
    },

    {
        __LINE__,
        "SELECT UserID, RegionID FROM test.visits_all WHERE CounterID GLOBAL IN (SELECT CounterID FROM test.visits_all WHERE BrowserID IN (SELECT BrowserID FROM test.visits_all WHERE OtherID = 1))",
        "SELECT UserID, RegionID FROM test.visits_all WHERE CounterID GLOBAL IN (SELECT CounterID FROM test.visits_all WHERE BrowserID IN (SELECT BrowserID FROM test.visits_all WHERE OtherID = 1))",
        2,
        DB::DistributedProductMode::LOCAL,
        true
    },

    {
        __LINE__,
        "SELECT UserID, RegionID FROM test.visits_all WHERE CounterID IN (SELECT CounterID FROM test.visits_all WHERE BrowserID IN (SELECT BrowserID FROM remote_db.remote_visits WHERE OtherID = 1))",
        "SELECT UserID, RegionID FROM test.visits_all WHERE CounterID IN (SELECT CounterID FROM remote_db.remote_visits WHERE BrowserID IN (SELECT BrowserID FROM remote_db.remote_visits WHERE OtherID = 1))",
        2,
        DB::DistributedProductMode::LOCAL,
        true
    },

    /// Section JOIN / depth 2

    {
        __LINE__,
        "SELECT UserID, RegionID FROM test.visits_all WHERE CounterID IN (SELECT CounterID FROM test.visits_all ALL INNER JOIN (SELECT CounterID FROM (SELECT CounterID FROM test.visits_all)) USING CounterID)",
        "SELECT UserID, RegionID FROM test.visits_all WHERE CounterID IN (SELECT CounterID FROM test.visits_all ALL INNER JOIN (SELECT CounterID FROM (SELECT CounterID FROM test.visits_all)) USING CounterID)",
        2,
        DB::DistributedProductMode::ALLOW,
        true
    },

    {
        __LINE__,
        "SELECT UserID, RegionID FROM test.visits_all WHERE CounterID GLOBAL IN (SELECT CounterID FROM test.visits_all ALL INNER JOIN (SELECT CounterID FROM (SELECT CounterID FROM remote_db.remote_visits)) USING CounterID)",
        "SELECT UserID, RegionID FROM test.visits_all WHERE CounterID GLOBAL IN (SELECT CounterID FROM test.visits_all ALL INNER JOIN (SELECT CounterID FROM (SELECT CounterID FROM remote_db.remote_visits)) USING CounterID)",
        2,
        DB::DistributedProductMode::DENY,
        true
    },

    {
        __LINE__,
        "SELECT UserID, RegionID FROM test.visits_all WHERE CounterID GLOBAL IN (SELECT CounterID FROM test.visits_all ALL INNER JOIN (SELECT CounterID FROM (SELECT CounterID FROM test.visits_all)) USING CounterID)",
        "SELECT UserID, RegionID FROM test.visits_all WHERE CounterID GLOBAL IN (SELECT CounterID FROM test.visits_all ALL INNER JOIN (SELECT CounterID FROM (SELECT CounterID FROM test.visits_all)) USING CounterID)",
        2,
        DB::DistributedProductMode::DENY,
        true
    },

    {
        __LINE__,
        "SELECT UserID, RegionID FROM test.visits_all WHERE CounterID IN (SELECT CounterID FROM test.visits_all ALL INNER JOIN (SELECT CounterID FROM (SELECT CounterID FROM test.visits_all)) USING CounterID)",
        "SELECT UserID, RegionID FROM test.visits_all WHERE CounterID GLOBAL IN (SELECT CounterID FROM test.visits_all GLOBAL ALL INNER JOIN (SELECT CounterID FROM (SELECT CounterID FROM test.visits_all)) USING CounterID)",
        2,
        DB::DistributedProductMode::GLOBAL,
        true
    },

    {
        __LINE__,
        "SELECT UserID, RegionID FROM test.visits_all WHERE CounterID IN (SELECT CounterID FROM test.visits_all ALL INNER JOIN (SELECT CounterID FROM (SELECT CounterID FROM test.visits_all)) USING CounterID)",
        "SELECT UserID, RegionID FROM test.visits_all WHERE CounterID IN (SELECT CounterID FROM remote_db.remote_visits ALL INNER JOIN (SELECT CounterID FROM (SELECT CounterID FROM remote_db.remote_visits)) USING CounterID)",
        2,
        DB::DistributedProductMode::LOCAL,
        true
    },

    /// Section JOIN / depth 2

    {
        __LINE__,
        "SELECT UserID FROM test.visits_all WHERE OtherID IN (SELECT OtherID FROM (SELECT OtherID FROM test.visits_all WHERE RegionID = 1) ALL INNER JOIN (SELECT OtherID FROM test.visits_all WHERE RegionID = 2) USING OtherID)",
        "SELECT UserID FROM test.visits_all WHERE OtherID IN (SELECT OtherID FROM (SELECT OtherID FROM test.visits_all WHERE RegionID = 1) ALL INNER JOIN (SELECT OtherID FROM test.visits_all WHERE RegionID = 2) USING OtherID)",
        2,
        DB::DistributedProductMode::ALLOW,
        true
    },

    {
        __LINE__,
        "SELECT UserID FROM test.visits_all WHERE OtherID GLOBAL IN (SELECT OtherID FROM (SELECT OtherID FROM test.visits_all WHERE RegionID = 1) ALL INNER JOIN (SELECT OtherID FROM remote_db.remote_visits WHERE RegionID = 2) USING OtherID)",
        "SELECT UserID FROM test.visits_all WHERE OtherID GLOBAL IN (SELECT OtherID FROM (SELECT OtherID FROM test.visits_all WHERE RegionID = 1) ALL INNER JOIN (SELECT OtherID FROM remote_db.remote_visits WHERE RegionID = 2) USING OtherID)",
        2,
        DB::DistributedProductMode::DENY,
        true
    },

    {
        __LINE__,
        "SELECT UserID FROM test.visits_all WHERE OtherID GLOBAL IN (SELECT OtherID FROM (SELECT OtherID FROM test.visits_all WHERE RegionID = 1) ALL INNER JOIN (SELECT OtherID FROM test.visits_all WHERE RegionID = 2) USING OtherID)",
        "SELECT UserID FROM test.visits_all WHERE OtherID GLOBAL IN (SELECT OtherID FROM (SELECT OtherID FROM test.visits_all WHERE RegionID = 1) ALL INNER JOIN (SELECT OtherID FROM test.visits_all WHERE RegionID = 2) USING OtherID)",
        2,
        DB::DistributedProductMode::DENY,
        true
    },

    {
        __LINE__,
        "SELECT UserID FROM test.visits_all WHERE OtherID IN (SELECT OtherID FROM (SELECT OtherID FROM test.visits_all WHERE RegionID = 1) ALL INNER JOIN (SELECT OtherID FROM test.visits_all WHERE RegionID = 2) USING OtherID)",
        "SELECT UserID FROM test.visits_all WHERE OtherID IN (SELECT OtherID FROM (SELECT OtherID FROM test.visits_all WHERE RegionID = 1) ALL INNER JOIN (SELECT OtherID FROM test.visits_all WHERE RegionID = 2) USING OtherID)",
        2,
        DB::DistributedProductMode::DENY,
        false
    },

    {
        __LINE__,
        "SELECT UserID FROM test.visits_all WHERE OtherID IN (SELECT OtherID FROM (SELECT OtherID FROM test.visits_all WHERE RegionID = 1) ALL INNER JOIN (SELECT OtherID FROM test.visits_all WHERE RegionID = 2) USING OtherID)",
        "SELECT UserID FROM test.visits_all WHERE OtherID GLOBAL IN (SELECT OtherID FROM (SELECT OtherID FROM test.visits_all WHERE RegionID = 1) GLOBAL ALL INNER JOIN (SELECT OtherID FROM test.visits_all WHERE RegionID = 2) USING OtherID)",
        2,
        DB::DistributedProductMode::GLOBAL,
        true
    },

    {
        __LINE__,
        "SELECT UserID FROM test.visits_all WHERE OtherID IN (SELECT OtherID FROM (SELECT OtherID FROM test.visits_all WHERE RegionID = 1) ALL INNER JOIN (SELECT OtherID FROM test.visits_all WHERE RegionID = 2) USING OtherID)",
        "SELECT UserID FROM test.visits_all WHERE OtherID IN (SELECT OtherID FROM (SELECT OtherID FROM remote_db.remote_visits WHERE RegionID = 1) ALL INNER JOIN (SELECT OtherID FROM remote_db.remote_visits WHERE RegionID = 2) USING OtherID)",
        2,
        DB::DistributedProductMode::LOCAL,
        true
    },

    /// Section JOIN / section IN

    {
        __LINE__,
        "SELECT UserID FROM test.visits_all ALL INNER JOIN (SELECT UserID FROM test.visits_all WHERE OtherID IN (SELECT OtherID FROM test.visits_all WHERE RegionID = 2)) USING UserID",
        "SELECT UserID FROM test.visits_all ALL INNER JOIN (SELECT UserID FROM test.visits_all WHERE OtherID IN (SELECT OtherID FROM test.visits_all WHERE RegionID = 2)) USING UserID",
        2,
        DB::DistributedProductMode::ALLOW,
        true
    },

    {
        __LINE__,
        "SELECT UserID FROM test.visits_all ALL INNER JOIN (SELECT UserID FROM test.visits_all WHERE OtherID IN (SELECT OtherID FROM test.visits_all WHERE RegionID = 2)) USING UserID",
        "SELECT UserID FROM test.visits_all ALL INNER JOIN (SELECT UserID FROM test.visits_all WHERE OtherID IN (SELECT OtherID FROM test.visits_all WHERE RegionID = 2)) USING UserID",
        2,
        DB::DistributedProductMode::DENY,
        false
    },

    {
        __LINE__,
        "SELECT UserID FROM test.visits_all GLOBAL ALL INNER JOIN (SELECT UserID FROM test.visits_all WHERE OtherID IN (SELECT OtherID FROM test.visits_all WHERE RegionID = 2)) USING UserID",
        "SELECT UserID FROM test.visits_all GLOBAL ALL INNER JOIN (SELECT UserID FROM test.visits_all WHERE OtherID IN (SELECT OtherID FROM test.visits_all WHERE RegionID = 2)) USING UserID",
        2,
        DB::DistributedProductMode::DENY,
        true
    },

    {
        __LINE__,
        "SELECT UserID FROM test.visits_all GLOBAL ALL INNER JOIN (SELECT UserID FROM test.visits_all WHERE OtherID IN (SELECT OtherID FROM remote_db.remote_visits WHERE RegionID = 2)) USING UserID",
        "SELECT UserID FROM test.visits_all GLOBAL ALL INNER JOIN (SELECT UserID FROM test.visits_all WHERE OtherID IN (SELECT OtherID FROM remote_db.remote_visits WHERE RegionID = 2)) USING UserID",
        2,
        DB::DistributedProductMode::DENY,
        true
    },

    {
        __LINE__,
        "SELECT UserID FROM test.visits_all ALL INNER JOIN (SELECT UserID FROM test.visits_all WHERE OtherID IN (SELECT OtherID FROM test.visits_all WHERE RegionID = 2)) USING UserID",
        "SELECT UserID FROM test.visits_all ALL INNER JOIN (SELECT UserID FROM remote_db.remote_visits WHERE OtherID IN (SELECT OtherID FROM remote_db.remote_visits WHERE RegionID = 2)) USING UserID",
        2,
        DB::DistributedProductMode::LOCAL,
        true
    },

    /// Table function.

    {
        __LINE__,
        "SELECT count() FROM remote('127.0.0.{1,2}', test, visits_all) WHERE UserID IN (SELECT UserID FROM test.visits_all)",
        "SELECT count() FROM remote('127.0.0.{1,2}', test, visits_all) WHERE UserID IN (SELECT UserID FROM test.visits_all)",
        2,
        DB::DistributedProductMode::ALLOW,
        true
    },

    {
        __LINE__,
        "SELECT count() FROM remote('127.0.0.{1,2}', test, visits_all) WHERE UserID IN (SELECT UserID FROM test.visits_all)",
        "SELECT count() FROM remote('127.0.0.{1,2}', test, visits_all) WHERE UserID IN (SELECT UserID FROM test.visits_all)",
        2,
        DB::DistributedProductMode::DENY,
        true
    },

    {
        __LINE__,
        "SELECT count() FROM test.visits_all WHERE UserID IN (SELECT UserID FROM remote('127.0.0.{1,2}', test, visits_all))",
        "SELECT count() FROM test.visits_all WHERE UserID IN (SELECT UserID FROM remote('127.0.0.{1,2}', test, visits_all))",
        2,
        DB::DistributedProductMode::DENY,
        true
    },

    {
        __LINE__,
        "SELECT count() FROM remote('127.0.0.{1,2}', test, visits_all) WHERE UserID IN (SELECT UserID FROM test.visits_all)",
        "SELECT count() FROM remote('127.0.0.{1,2}', test, visits_all) WHERE UserID IN (SELECT UserID FROM test.visits_all)",
        2,
        DB::DistributedProductMode::GLOBAL,
        true
    },

    {
        __LINE__,
        "SELECT count() FROM test.visits_all WHERE UserID IN (SELECT UserID FROM remote('127.0.0.{1,2}', test, visits_all))",
        "SELECT count() FROM test.visits_all WHERE UserID IN (SELECT UserID FROM remote('127.0.0.{1,2}', test, visits_all))",
        2,
        DB::DistributedProductMode::GLOBAL,
        true
    },

    {
        __LINE__,
        "SELECT count() FROM remote('127.0.0.{1,2}', test, visits_all) WHERE UserID IN (SELECT UserID FROM test.visits_all)",
        "SELECT count() FROM remote('127.0.0.{1,2}', test, visits_all) WHERE UserID IN (SELECT UserID FROM test.visits_all)",
        2,
        DB::DistributedProductMode::LOCAL,
        true
    },

    {
        __LINE__,
        "SELECT count() FROM test.visits_all WHERE UserID IN (SELECT UserID FROM remote('127.0.0.{1,2}', test, visits_all))",
        "SELECT count() FROM test.visits_all WHERE UserID IN (SELECT UserID FROM remote('127.0.0.{1,2}', test, visits_all))",
        2,
        DB::DistributedProductMode::LOCAL,
        true
    },

    /// Section IN / depth 2 / two distributed tables

    {
        __LINE__,
        "SELECT UserID, RegionID FROM test.visits_all WHERE CounterID IN (SELECT CounterID FROM test.hits_all WHERE BrowserID IN (SELECT BrowserID FROM test.visits_all WHERE OtherID = 1))",
        "SELECT UserID, RegionID FROM test.visits_all WHERE CounterID IN (SELECT CounterID FROM distant_db.distant_hits WHERE BrowserID IN (SELECT BrowserID FROM remote_db.remote_visits WHERE OtherID = 1))",
        2,
        DB::DistributedProductMode::LOCAL,
        true
    },

    /// Aggregate function.

    {
        __LINE__,
        "SELECT sum(RegionID IN (SELECT RegionID from test.hits_all)) FROM test.visits_all",
        "SELECT sum(RegionID IN (SELECT RegionID from test.hits_all)) FROM test.visits_all",
        2,
        DB::DistributedProductMode::ALLOW,
        true
    },

    {
        __LINE__,
        "SELECT sum(RegionID IN (SELECT RegionID from test.hits_all)) FROM test.visits_all",
        "SELECT sum(RegionID IN (SELECT RegionID from test.hits_all)) FROM test.visits_all",
        2,
        DB::DistributedProductMode::DENY,
        false
    },

    {
        __LINE__,
        "SELECT sum(RegionID GLOBAL IN (SELECT RegionID from test.hits_all)) FROM test.visits_all",
        "SELECT sum(RegionID GLOBAL IN (SELECT RegionID from test.hits_all)) FROM test.visits_all",
        2,
        DB::DistributedProductMode::DENY,
        true
    },

    {
        __LINE__,
        "SELECT sum(RegionID IN (SELECT RegionID from test.hits_all)) FROM test.visits_all",
        "SELECT sum(RegionID GLOBAL IN (SELECT RegionID from test.hits_all)) FROM test.visits_all",
        2,
        DB::DistributedProductMode::GLOBAL,
        true
    },

    {
        __LINE__,
        "SELECT sum(RegionID GLOBAL IN (SELECT RegionID from test.hits_all)) FROM test.visits_all",
        "SELECT sum(RegionID GLOBAL IN (SELECT RegionID from test.hits_all)) FROM test.visits_all",
        2,
        DB::DistributedProductMode::LOCAL,
        true
    },

    {
        __LINE__,
        "SELECT sum(RegionID IN (SELECT RegionID from test.hits_all)) FROM test.visits_all",
        "SELECT sum(RegionID IN (SELECT RegionID from distant_db.distant_hits)) FROM test.visits_all",
        2,
        DB::DistributedProductMode::LOCAL,
        true
    },

    /// Miscellaneous.

    {
        __LINE__,
        "SELECT count() FROM test.visits_all WHERE x GLOBAL IN (SELECT x FROM test.visits_all WHERE x GLOBAL IN (SELECT x FROM test.visits_all))",
        "SELECT count() FROM test.visits_all WHERE x GLOBAL IN (SELECT x FROM test.visits_all WHERE x GLOBAL IN (SELECT x FROM test.visits_all))",
        2,
        DB::DistributedProductMode::DENY,
        true
    },

    {
        __LINE__,
        "SELECT count() FROM test.visits_all WHERE x GLOBAL IN (SELECT x FROM test.visits_all WHERE x GLOBAL IN (SELECT x FROM test.visits_all))",
        "SELECT count() FROM test.visits_all WHERE x GLOBAL IN (SELECT x FROM test.visits_all WHERE x GLOBAL IN (SELECT x FROM test.visits_all))",
        2,
        DB::DistributedProductMode::LOCAL,
        true
    },

    {
        __LINE__,
        "SELECT count() FROM test.visits_all WHERE x GLOBAL IN (SELECT x FROM test.visits_all WHERE x GLOBAL IN (SELECT x FROM test.visits_all))",
        "SELECT count() FROM test.visits_all WHERE x GLOBAL IN (SELECT x FROM test.visits_all WHERE x GLOBAL IN (SELECT x FROM test.visits_all))",
        2,
        DB::DistributedProductMode::GLOBAL,
        true
    },

    {
        __LINE__,
        "SELECT UserID FROM (SELECT UserID FROM test.visits_all WHERE UserID GLOBAL IN (SELECT UserID FROM test.hits_all))",
        "SELECT UserID FROM (SELECT UserID FROM test.visits_all WHERE UserID GLOBAL IN (SELECT UserID FROM test.hits_all))",
        2,
        DB::DistributedProductMode::DENY,
        true
    }
};


static bool run()
{
    unsigned int count = 0;
    unsigned int i = 1;

    for (const auto & entry : entries)
    {
        auto res = check(entry);
        if (res.first)
        {
            ++count;
        }
        else
            std::cout << "Test " << i << " at line " << entry.line_num << " failed.\n"
                "Expected: " << entry.expected_output << ".\n"
                "Received: " << res.second << "\n";

        ++i;
    }

    std::cout << count << " out of " << entries.size() << " test(s) passed.\n";

    return count == entries.size();
}


TestResult check(const TestEntry & entry)
{
    static DB::SharedContextHolder shared_context = DB::Context::createShared();
    static DB::Context context = DB::Context::createGlobal(shared_context.get());
    context.makeGlobalContext();

    try
    {

        auto storage_distributed_visits = StorageDistributedFake::create("remote_db", "remote_visits", entry.shard_count);
        auto storage_distributed_hits = StorageDistributedFake::create("distant_db", "distant_hits", entry.shard_count);

        DB::DatabasePtr database = std::make_shared<DB::DatabaseOrdinary>("test", "./metadata/test/", context);
        DB::DatabaseCatalog::instance().attachDatabase("test", database);
        database->attachTable("visits_all", storage_distributed_visits);
        database->attachTable("hits_all", storage_distributed_hits);
        context.setCurrentDatabase("test");
        context.setSetting("distributed_product_mode", entry.mode);

        /// Parse and process the incoming query.
        DB::ASTPtr ast_input;
        if (!parse(ast_input, entry.input))
            return TestResult(false, "parse error");

        bool success = true;

        try
        {
            DB::InJoinSubqueriesPreprocessor::SubqueryTables renamed;
            DB::InJoinSubqueriesPreprocessor(context, renamed, std::make_unique<CheckShardsAndTablesMock>()).visit(ast_input);
        }
        catch (const DB::Exception & ex)
        {
            if (ex.code() == DB::ErrorCodes::DISTRIBUTED_IN_JOIN_SUBQUERY_DENIED)
                success = false;
            else
                throw;
        }
        catch (...)
        {
            throw;
        }

        if (success != entry.expected_success)
            return TestResult(false, "unexpected result");

        /// Parse the expected result.
        DB::ASTPtr ast_expected;
        if (!parse(ast_expected, entry.expected_output))
            return TestResult(false, "parse error");

        /// Compare the processed query and the expected result.
        bool res = equals(ast_input, ast_expected);
        std::string output = DB::queryToString(ast_input);

        DB::DatabaseCatalog::instance().detachDatabase("test");
        return TestResult(res, output);
    }
    catch (DB::Exception & e)
    {
        DB::DatabaseCatalog::instance().detachDatabase("test");
        return TestResult(false, e.displayText());
    }
}

bool parse(DB::ASTPtr & ast, const std::string & query)
{
    DB::ParserSelectQuery parser;
    std::string message;
    const auto * begin = query.data();
    const auto * end = begin + query.size();
    ast = DB::tryParseQuery(parser, begin, end, message, false, "", false, 0, 0);
    return ast != nullptr;
}

bool equals(const DB::ASTPtr & lhs, const DB::ASTPtr & rhs)
{
    DB::ASTPtr lhs_reordered = lhs->clone();
    reorder(&*lhs_reordered);

    DB::ASTPtr rhs_reordered = rhs->clone();
    reorder(&*rhs_reordered);

    return lhs_reordered->getTreeHash() == rhs_reordered->getTreeHash();
}

void reorder(DB::IAST * ast)
{
    if (ast == nullptr)
        return;

    auto & children = ast->children;
    if (children.empty())
        return;

    for (auto & child : children)
        reorder(&*child);

    std::sort(children.begin(), children.end(), [](const DB::ASTPtr & lhs, const DB::ASTPtr & rhs)
    {
        return lhs->getTreeHash() < rhs->getTreeHash();
    });
}

int main()
{
    return run() ? EXIT_SUCCESS : EXIT_FAILURE;
}
