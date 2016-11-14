#include <ext/shared_ptr_helper.hpp>

#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/Parsers/ParserSelectQuery.h>
#include <DB/Parsers/parseQuery.h>
#include <DB/Parsers/queryToString.h>
#include <DB/Interpreters/InJoinSubqueriesPreprocessor.h>
#include <DB/Interpreters/Context.h>
#include <DB/Interpreters/Settings.h>
#include <DB/Storages/IStorage.h>
#include <DB/Databases/IDatabase.h>
#include <DB/Databases/DatabaseOrdinary.h>

#include <iostream>
#include <vector>
#include <utility>
#include <string>

/// Упрощённый вариант класса StorageDistributed.
class StorageDistributedFake : private ext::shared_ptr_helper<StorageDistributedFake>, public DB::IStorage
{
friend class ext::shared_ptr_helper<StorageDistributedFake>;

public:
	static DB::StoragePtr create(const std::string & remote_database_, const std::string & remote_table_, size_t shard_count_)
	{
		return make_shared(remote_database_, remote_table_, shard_count_);
	}

	std::string getName() const override { return "DistributedFake"; }
	bool isRemote() const override { return true; }
	size_t getShardCount() const { return shard_count; }
	std::string getRemoteDatabaseName() const { return remote_database; }
	std::string getRemoteTableName() const { return remote_table; }

	std::string getTableName() const override { return ""; }
	const DB::NamesAndTypesList & getColumnsListImpl() const override { return names_and_types; }

private:
	StorageDistributedFake(const std::string & remote_database_, const std::string & remote_table_, size_t shard_count_)
		: remote_database(remote_database_), remote_table(remote_table_), shard_count(shard_count_)
	{
	}

private:
	const std::string remote_database;
	const std::string remote_table;
	size_t shard_count;
	DB::NamesAndTypesList names_and_types;
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
bool parse(DB::ASTPtr  & ast, const std::string & query);
bool equals(const DB::ASTPtr & lhs, const DB::ASTPtr & rhs);
void reorder(DB::IAST * ast);

TestEntries entries =
{
	/// Тривиальный запрос.

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

	/// Секция IN / глубина 1

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

	/// Секция NOT IN / глубина 1

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

	/// Секция GLOBAL IN / глубина 1

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

	/// Секция GLOBAL NOT IN / глубина 1

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

	/// Секция JOIN / глубина 1

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

	/// Секция GLOBAL JOIN / глубина 1

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

	/// Секция JOIN / глубина 1 / 2 подзапроса.

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

	/// Секция IN / глубина 1 / таблица на уровне 2

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

	/// Секция GLOBAL IN / глубина 1 / таблица на уровне 2

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

	/// Секция IN на уровне 1, секция GLOBAL IN на уровне 2.

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
		"SELECT UserID FROM test.visits_all WHERE UserID IN (SELECT UserID FROM remote_db.remote_visits WHERE UserID GLOBAL IN (SELECT UserID FROM (SELECT UserID FROM test.visits_all)))",
		2,
		DB::DistributedProductMode::GLOBAL,
		true
	},

	{
		__LINE__,
		"SELECT UserID FROM test.visits_all WHERE UserID IN (SELECT UserID FROM remote_db.remote_visits WHERE UserID GLOBAL IN (SELECT UserID FROM (SELECT UserID FROM test.visits_all)))",
		"SELECT UserID FROM test.visits_all WHERE UserID IN (SELECT UserID FROM remote_db.remote_visits WHERE UserID GLOBAL IN (SELECT UserID FROM (SELECT UserID FROM remote_db.remote_visits)))",
		2,
		DB::DistributedProductMode::LOCAL,
		true
	},

	/// Секция JOIN / глубина 1 / таблица на уровне 2

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

	/// Секция IN / глубина 2

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
		"SELECT UserID, RegionID FROM test.visits_all WHERE CounterID IN (SELECT CounterID FROM test.visits_all WHERE BrowserID IN (SELECT BrowserID FROM test.visits_all WHERE OtherID = 1))",
		"SELECT UserID, RegionID FROM test.visits_all WHERE CounterID GLOBAL IN (SELECT CounterID FROM test.visits_all WHERE BrowserID GLOBAL IN (SELECT BrowserID FROM test.visits_all WHERE OtherID = 1))",
		2,
		DB::DistributedProductMode::GLOBAL,
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

	/// Секция JOIN / глубина 2

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

	/// Секция JOIN / глубина 2

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

	/// Секция JOIN / секция IN

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
		"SELECT UserID FROM test.visits_all GLOBAL ALL INNER JOIN (SELECT UserID FROM test.visits_all WHERE OtherID GLOBAL IN (SELECT OtherID FROM test.visits_all WHERE RegionID = 2)) USING UserID",
		2,
		DB::DistributedProductMode::GLOBAL,
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

	/// Табличная функция.

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

	/// Секция IN / глубина 2 / две распределённые таблицы

	{
		__LINE__,
		"SELECT UserID, RegionID FROM test.visits_all WHERE CounterID IN (SELECT CounterID FROM test.hits_all WHERE BrowserID IN (SELECT BrowserID FROM test.visits_all WHERE OtherID = 1))",
		"SELECT UserID, RegionID FROM test.visits_all WHERE CounterID GLOBAL IN (SELECT CounterID FROM test.hits_all WHERE BrowserID GLOBAL IN (SELECT BrowserID FROM test.visits_all WHERE OtherID = 1))",
		2,
		DB::DistributedProductMode::GLOBAL,
		true
	},

	{
		__LINE__,
		"SELECT UserID, RegionID FROM test.visits_all WHERE CounterID IN (SELECT CounterID FROM test.hits_all WHERE BrowserID IN (SELECT BrowserID FROM test.visits_all WHERE OtherID = 1))",
		"SELECT UserID, RegionID FROM test.visits_all WHERE CounterID IN (SELECT CounterID FROM distant_db.distant_hits WHERE BrowserID IN (SELECT BrowserID FROM remote_db.remote_visits WHERE OtherID = 1))",
		2,
		DB::DistributedProductMode::LOCAL,
		true
	},

	/// Агрегатная функция.

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


bool performTests(const TestEntries & entries)
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

bool run()
{
	return performTests(entries);
}

TestResult check(const TestEntry & entry)
{
	try
	{
		DB::Context context;

		auto storage_distributed_visits = StorageDistributedFake::create("remote_db", "remote_visits", entry.shard_count);
		auto storage_distributed_hits = StorageDistributedFake::create("distant_db", "distant_hits", entry.shard_count);

		DB::DatabasePtr database = std::make_shared<DB::DatabaseOrdinary>("test", "./metadata/test/");
		context.addDatabase("test", database);
		database->attachTable("visits_all", storage_distributed_visits);
		database->attachTable("hits_all", storage_distributed_hits);
		context.setCurrentDatabase("test");

		auto & settings = context.getSettingsRef();
		settings.distributed_product_mode = entry.mode;

		/// Парсить и обработать входящий запрос.
		DB::ASTPtr ast_input;
		if (!parse(ast_input, entry.input))
			return TestResult(false, "parse error");

		auto select_query = typeid_cast<DB::ASTSelectQuery *>(&*ast_input);

		bool success = true;

		try
		{
			DB::InJoinSubqueriesPreprocessor<StorageDistributedFake> preprocessor(select_query, context, storage_distributed_visits);
			preprocessor.perform();
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

		/// Парсить ожидаемый результат.
		DB::ASTPtr ast_expected;
		if (!parse(ast_expected, entry.expected_output))
			return TestResult(false, "parse error");

		/// Сравнить обработанный запрос и ожидаемый результат.
		bool res = equals(ast_input, ast_expected);
		std::string output = DB::queryToString(ast_input);

		return TestResult(res, output);
	}
	catch (DB::Exception & e)
	{
		return TestResult(false, e.displayText());
	}
}

bool parse(DB::ASTPtr & ast, const std::string & query)
{
	DB::ParserSelectQuery parser;
	std::string message;
	auto begin = query.data();
	auto end = begin + query.size();
	ast = DB::tryParseQuery(parser, begin, end, message, false, "", false);
	return ast != nullptr;
}

bool equals(const DB::ASTPtr & lhs, const DB::ASTPtr & rhs)
{
	DB::ASTPtr lhs_reordered = lhs->clone();
	reorder(&*lhs_reordered);

	DB::ASTPtr rhs_reordered = rhs->clone();
	reorder(&*rhs_reordered);

	return lhs_reordered->getTreeID() == rhs_reordered->getTreeID();
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
		return lhs->getTreeID() < rhs->getTreeID();
	});
}

int main()
{
	return run() ? EXIT_SUCCESS : EXIT_FAILURE;
}
