#include "gtest_fdb_common.h"

#if USE_FDB

#include <cstdlib>
#include <cstring>
#include <foundationdb/fdb_c.h>
#include <gtest/gtest.h>
#include <Common/FoundationDB/FoundationDBCommon.h>
#include <Common/FoundationDB/FoundationDBHelpers.h>
#include <Common/getRandomASCIIString.h>

namespace
{
// Shutdown fdb network
struct FDBNetworkEnvironment : public ::testing::Environment
{
    void TearDown() override { DB::FoundationDBNetwork::shutdownIfNeed(); }
};

const auto * fdb_network_env = testing::AddGlobalTestEnvironment(new FDBNetworkEnvironment());
}

namespace DB
{

void FDBFixture::SetUpTestSuite()
{
    auto * cluster_file_env = std::getenv("CLICKHOUSE_UT_FDB_CLUSTER_FILE"); // NOLINT(concurrency-mt-unsafe)
    if (!cluster_file_env || cluster_file_env[0] == 0)
        return;

    cluster_file = cluster_file_env;
    key_prefix = "ch_ut_" + getRandomASCIIString(5) + "/";

    FoundationDBNetwork::ensureStarted();
    throwIfFDBError(fdb_create_database(cluster_file.c_str(), &db));
    clear();
}

void FDBFixture::TearDownTestSuite()
{
    if (db)
    {
        clear();
        fdb_database_destroy(db);
    }
}

void FDBFixture::clear()
{
    if (!db)
        return;

    FDBTransaction * tr;
    throwIfFDBError(fdb_database_create_transaction(db, &tr));
    auto tr_ptr = fdb_manage_object(tr);

    static int64_t timeout = 5000; // ms
    throwIfFDBError(fdb_transaction_set_option(tr, FDB_TR_OPTION_TIMEOUT, reinterpret_cast<const uint8_t *>(&timeout), 8));

    auto range_start = std::string(key_prefix);
    auto range_end = range_start + "\xff";
    fdb_transaction_clear_range(tr, FDB_KEY_FROM_STRING(range_start), FDB_KEY_FROM_STRING(range_end));

    auto future = fdb_manage_object(fdb_transaction_commit(tr));
    throwIfFDBError(fdb_future_block_until_ready(future.get()));
    throwIfFDBError(fdb_future_get_error(future.get()));
}

std::string FDBFixture::cluster_file;
std::string FDBFixture::key_prefix;
FDBDatabase * FDBFixture::db = nullptr;
}
#endif
