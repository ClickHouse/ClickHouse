#include "config.h"

#if USE_FDB

#include <chrono>
#include <thread>
#include <tuple>
#include <type_traits>
#include <foundationdb/fdb_c.h>
#include <foundationdb/fdb_c_types.h>
#include <gtest/gtest.h>
#include <Common/FoundationDB/FoundationDBCommon.h>
#include <Common/FoundationDB/fdb_c_fwd.h>

#include "gtest_fdb_common.h"

using namespace std::chrono_literals;

namespace DB::FoundationDB
{
class FDBCExtensionTests : public FDBFixture
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
        fdb_future_set_callback(future, fdb_callback, new std::function<void()>(callback));
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

TEST_F(FDBCExtensionTests, DelayShouldWork)
{
    auto before = std::chrono::system_clock::now();

    auto * future = fdb_delay(2);
    fdb_future_block_until_ready(future);
    fdb_future_destroy(future);

    auto after = std::chrono::system_clock::now();
    ASSERT_GE(after - before, 2s);
}

TEST_F(FDBCExtensionTests, RWLockShouldWork)
{
    auto * lock_a = fdb_rwlocks_create();
    fdb_rwlocks_shared(lock_a, "/a");
    fdb_rwlocks_exclusive(lock_a, "/b");
    wait_then_destroy(fdb_rwlocks_lock(lock_a));
    fdb_rwlocks_free(lock_a);
}

// order, lock types
// e.g. "01234", {"R1W2R3", "R0R2W3"}
class FDBRWLockOrderTests : public FDBCExtensionTests, public testing::WithParamInterface<std::tuple<std::string, std::vector<std::string>>>
{
};

TEST_P(FDBRWLockOrderTests, RWLockOrderShouldCorrect)
{
    const auto & [expect_lock_times, lock_types] = GetParam();
    ASSERT_EQ(lock_types.size(), expect_lock_times.length());
    if (lock_types.empty())
        return;

    std::vector<std::tuple<FDBRWLockHodler *, FDBFuture *>> locks;
    std::vector<std::chrono::system_clock::time_point> lock_times;
    std::vector<std::chrono::system_clock::time_point> unlock_times;

    // Lock all
    for (const auto & lock_type : lock_types)
    {
        auto * lock = fdb_rwlocks_create();

        if (lock_type.size() % 2 != 0)
        {
            fdb_rwlocks_free(lock);
            ASSERT_TRUE(false) << "Illegal lock type: " << lock_type;
        }

        for (int i = 0; i < lock_type.size(); i += 2)
        {
            std::string key = "key";
            key.push_back(lock_type[i + 1]);

            if (lock_type[i] == 'R')
                fdb_rwlocks_shared(lock, key.data());
            else if (lock_type[i] == 'W')
                fdb_rwlocks_exclusive(lock, key.data());
            else
            {
                fdb_rwlocks_free(lock);
                ASSERT_TRUE(false) << "Illegal lock type: " << lock_type;
            }
        }

        lock_times.emplace_back();
        auto * f = fdb_rwlocks_lock(lock);
        callback(f, [&lock_times, i = lock_times.size() - 1] { lock_times[i] = std::chrono::system_clock::now(); });
        locks.emplace_back(lock, f);
    }

    // Wait lock, sleep then unlock
    for (auto & [lock, f] : locks)
    {
        wait_then_destroy(f);
        std::this_thread::sleep_for(1ms);
        fdb_rwlocks_free(lock);
        unlock_times.emplace_back(std::chrono::system_clock::now());
    }

    // Assert lock times
    auto should_after = std::chrono::system_clock::from_time_t(0);
    auto should_before = unlock_times[0];
    for (int i = 0; i < expect_lock_times.size(); i++)
    {
        if (i > 0 && expect_lock_times[i] > expect_lock_times[i - 1])
        {
            should_after = unlock_times[i - 1];
            should_before = unlock_times[i];
        }

        ASSERT_LT(should_after, lock_times[i]);
        ASSERT_LT(lock_times[i], should_before);
    }
}

static std::remove_cvref_t<decltype(std::declval<FDBRWLockOrderTests>().GetParam())>
singleLockOrder(std::string && order, std::string && single_locks)
{
    std::vector<std::string> locks;
    for (auto & single_lock : single_locks)
        locks.emplace_back(std::string() + single_lock + "1");
    return {order, locks};
}

INSTANTIATE_TEST_SUITE_P(
    FDBRWLockOrderSingle,
    FDBRWLockOrderTests,
    testing::Values(
        singleLockOrder("00", "RR"),
        singleLockOrder("01", "WW"),
        singleLockOrder("01", "WR"),
        singleLockOrder("01", "RW"),
        singleLockOrder("0000", "RRRR"),
        singleLockOrder("0123", "WWWW")));

static std::remove_cvref_t<decltype(std::declval<FDBRWLockOrderTests>().GetParam())>
multiLockOrder(std::string && order, std::vector<std::string> && locks)
{
    return {order, locks};
}

INSTANTIATE_TEST_SUITE_P(
    FDBRWLockOrderMulti,
    FDBRWLockOrderTests,
    testing::Values(
        multiLockOrder("00", {"R1R2", "R2R3"}),
        multiLockOrder("01", {"R1W2", "R1W2"}),
        multiLockOrder("01", {"R1W2", "R1R2"}),
        multiLockOrder("01", {"R1R2", "R1W2"}),
        multiLockOrder("01", {"R1R1W1", "R1R1R1"}),
        multiLockOrder("001", {"R1R2W3", "R1R2W4", "R1W2"}),
        multiLockOrder("01234567", {"W1", "W1", "W1", "R1W2", "R1W2", "R1W2", "W1", "W1"}),
        multiLockOrder("0123", {"W1", "R1W2", "W1", "R1W2"}),
        multiLockOrder("012", {"R1W2", "W1W2", "R1W2"}),
        multiLockOrder("0000", {"R1R2", "R2R3", "R3R1", "R5R1"})));
}
#endif
