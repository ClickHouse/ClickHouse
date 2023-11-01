#include "config.h"

#if USE_FDB

#include <future>
#include <memory>
#include <foundationdb/fdb_c.h>
#include <foundationdb/fdb_c_types.h>
#include <gtest/gtest.h>
#include "Common/FoundationDB/FoundationDBHelpers.h"
#include <Common/FoundationDB/FoundationDBCommon.h>
#include <Common/FoundationDB/fdb_c_fwd.h>

#include "gtest_fdb_common.h"

using namespace std::chrono_literals;

namespace DB::FoundationDB
{
class FDBCTests : public FDBFixture
{
public:
    void SetUp() override
    {
        SKIP_IF_NO_FDB();
        clear();
        throwIfFDBError(fdb_create_database(cluster_file.c_str(), &db));
    }

    void TearDown() override
    {
        if (db)
            fdb_database_destroy(db);
    }

    FDBDatabase * db = nullptr;

    void wait_then_destroy(FDBFuture * future)
    {
        fdb_future_block_until_ready(future);
        fdb_future_destroy(future);
    }

    template <typename CB>
    void callback(FDBFuture * future, CB callback)
    {
        throwIfFDBError(fdb_future_set_callback(future, fdb_callback, new std::function<void()>(callback)));
    }

    static void fdb_callback(FDBFuture *, void * payload)
    {
        auto * f = reinterpret_cast<std::function<void()> *>(payload);
        try
        {
            (*f)();
        }
        catch (const std::exception& e)
        {
            std::cerr << "Error: " << e.what() << std::endl;
        }
        delete f;
    }
};

TEST_F(FDBCTests, WatchClearRange)
{
    auto key = key_prefix + "key";

    // Create key
    {
        FDBTransaction * tr;
        throwIfFDBError(fdb_database_create_transaction(db, &tr));
        auto tr_guard = fdb_manage_object(tr);

        std::string data = "1";
        fdb_transaction_set(tr, FDB_KEY_FROM_STRING(key), FDB_VALUE_FROM_STRING(data));
        wait_then_destroy(fdb_transaction_commit(tr));
    }

    // Watch key
    auto wait_promise = std::make_shared<std::promise<void>>();
    auto wait_future = wait_promise->get_future();
    std::shared_ptr<FDBFuture> wait_guard;

    {
        FDBTransaction * tr_watch;
        throwIfFDBError(fdb_database_create_transaction(db, &tr_watch));
        auto tr_watch_guard = fdb_manage_object(tr_watch);

        wait_then_destroy(fdb_transaction_get(tr_watch, FDB_KEY_FROM_STRING(key), 0));

        auto * wait_f = fdb_transaction_watch(tr_watch, FDB_KEY_FROM_STRING(key));
        wait_guard = fdb_manage_object(wait_f);
        callback(wait_f, [wait_promise]() { wait_promise->set_value(); });
        wait_then_destroy(fdb_transaction_commit(tr_watch));
    }

    // Clear key
    {
        FDBTransaction * tr;
        throwIfFDBError(fdb_database_create_transaction(db, &tr));
        auto tr_guard = fdb_manage_object(tr);

        auto key_begin = key_prefix;
        auto key_end = key_begin + '\xff';
        fdb_transaction_clear_range(tr, FDB_KEY_FROM_STRING(key_begin), FDB_KEY_FROM_STRING(key_end));

        wait_then_destroy(fdb_transaction_commit(tr));
    }

    ASSERT_EQ(wait_future.wait_for(1s), std::future_status::ready);
}
}
#endif
