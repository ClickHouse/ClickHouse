#include "config.h"

#if USE_FDB

#include <exception>
#include <future>
#include <base/defines.h>
#include <foundationdb/fdb_c.h>
#include <gtest/gtest.h>
#include <Common/EventNotifier.h>
#include <Common/FoundationDB/FoundationDBCommon.h>
#include <Common/FoundationDB/FoundationDBHelpers.h>
#include <Common/FoundationDB/fdb_error_definitions.h>
#include <Common/FoundationDB/internal/Coroutine.h>
#include <Common/FoundationDB/internal/FDBExtended.h>
#include <Common/FoundationDB/internal/KeeperCommon.h>
#include <Common/FoundationDB/tests/gtest_fdb_common.h>

using namespace DB::FoundationDB::Coroutine;
using namespace std::chrono_literals;

namespace DB
{
class FDBCoroutineTest : public FDBFixture
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

protected:
    FDBDatabase * db = nullptr;
    Poco::Logger * log = &Poco::Logger::get("GTestFDBAsyncTrx");

    FDBTransaction * newTrx()
    {
        FDBTransaction * tr;
        throwIfFDBError(fdb_database_create_transaction(db, &tr));
        static int64_t timeout = 5000; // ms
        throwIfFDBError(fdb_transaction_set_option(tr, FDB_TR_OPTION_TIMEOUT, reinterpret_cast<const uint8_t *>(&timeout), 8));
        return tr;
    }

    static std::string key(const std::string & key) { return std::string(key_prefix) + key; }

    template <typename R>
    static R wait(std::future<R> & future)
    {
        return wait(future, 1s);
    }

    template <typename R, typename Duration>
    static R wait(std::future<R> & future, Duration duration)
    {
        if (future.wait_for(duration) != std::future_status::ready)
            throw std::runtime_error("future timeout");
        return future.get();
    }

    static auto runTask(auto && lambda)
    {
        using T = TaskReturnType<decltype(lambda())>;
        std::promise<T> promise;
        auto task = lambda();

        if constexpr (std::is_void_v<T>)
        {
            task.start(
                [&](std::exception_ptr eptr)
                {
                    if (eptr)
                        promise.set_exception(eptr);
                    else
                        promise.set_value();
                });
        }
        else
        {
            task.start(
                [&](const T & value, std::exception_ptr eptr)
                {
                    if (eptr)
                        promise.set_exception(eptr);
                    else
                        promise.set_value(value);
                });
        }

        auto future = promise.get_future();
        return wait(future);
    }
};

TEST_F(FDBCoroutineTest, CatchExceptionInCallback)
{
    /// NOTE: Invoke rvalue coroutine lambda is bad. See also: https://stackoverflow.com/questions/60592174/lambda-lifetime-explanation-for-c20-coroutines.
    auto fn_void = [&]() -> Task<void> { co_return; };
    auto task_void = fn_void();
    task_void.start([&](std::exception_ptr) { throw std::exception(); });

    auto fn_value = [&]() -> Task<bool> { co_return true; };
    auto task_value = fn_value();
    task_value.start([&](bool, std::exception_ptr) { throw std::exception(); });
}

TEST_F(FDBCoroutineTest, CommonReadWrite)
{
    auto set = [&](const std::string & key, const std::string & value) -> Task<void>
    {
        auto trx = fdb_manage_object(newTrx());
        fdb_transaction_set(trx.get(), FDB_KEY_FROM_STRING(key), FDB_VALUE_FROM_STRING(value));
        auto f = fdb_manage_object(fdb_transaction_commit(trx.get()));
        co_await *f;
        throwIfFDBError(fdb_future_get_error(f.get()));
    };

    auto get = [&](const std::string & key) -> Task<std::string>
    {
        auto trx = fdb_manage_object(newTrx());
        auto f = fdb_manage_object(fdb_transaction_get(trx.get(), FDB_KEY_FROM_STRING(key), true));
        co_await *f;
        throwIfFDBError(fdb_future_get_error(f.get()));

        fdb_bool_t present;
        const uint8_t * value;
        int value_length;

        throwIfFDBError(fdb_future_get_value(f.get(), &present, &value, &value_length));
        co_return std::string(reinterpret_cast<const char *>(value), value_length);
    };

    auto res = runTask(
        [set, get]() -> Task<std::string>
        {
            std::string key = key_prefix + "a";
            co_await set(key, "abc");
            co_return co_await get(key);
        });
    ASSERT_EQ(res, "abc");
}

TEST_F(FDBCoroutineTest, ExceptionInCoroutine)
{
    ASSERT_ANY_THROW(runTask(
        []() -> Task<void>
        {
            throw std::exception();
            co_return;
        }));
}

TEST_F(FDBCoroutineTest, CancelPendingFuture)
{
    auto delay = [](double second) -> Task<void>
    {
        auto f = fdb_manage_object(fdb_delay(second));
        co_await *f;
    };

    FoundationDB::AsyncTrxCancelSource source;
    std::promise<void> promise;
    auto future = promise.get_future();

    // Start delay task
    auto pure_delay_task = delay(5);
    pure_delay_task.start(
        [&](std::exception_ptr eptr)
        {
            if (eptr)
                promise.set_exception(eptr);
            else
                promise.set_value();
        },
        source.getToken());

    // Cancel after 2s
    std::this_thread::sleep_for(1s);
    source.cancel();
    try
    {
        wait(future);
        FAIL() << "No exceptions are thrown";
    }
    catch (const FoundationDBException & e)
    {
        ASSERT_EQ(e.code, FDBErrorCode::operation_cancelled);
    }
}

TEST_F(FDBCoroutineTest, CancelBeforeFuture)
{
    auto delay = [](double second) -> Task<fdb_error_t>
    {
        auto f = fdb_manage_object(fdb_delay(second));
        co_await *f;
        co_return fdb_future_get_error(f.get());
    };

    FoundationDB::AsyncTrxCancelSource source;
    source.cancel();
    std::promise<fdb_error_t> promise;
    auto future = promise.get_future();

    // Start delay task
    auto pure_delay_task = delay(5);
    pure_delay_task.start(
        [&](fdb_error_t err, std::exception_ptr eptr)
        {
            if (eptr)
                promise.set_exception(eptr);
            else
                promise.set_value(err);
        },
        source.getToken());
    ASSERT_THROW(wait(future), FoundationDB::KeeperException);
}

// TODO: The following Cancel* tests should not relay on Coroutine

TEST_F(FDBCoroutineTest, CancelPendingFutureWithTracker)
{
    auto delay = [](double second) -> Task<void> { co_await fdb_delay(second); };

    FoundationDB::AsyncTrxTracker tracker;

    std::promise<void> promise;
    delay(0.5).start(
        [&](std::exception_ptr eptr)
        {
            if (eptr)
                promise.set_exception(eptr);
            else
                promise.set_value();
        },
        tracker.newToken());

    tracker.gracefullyCancelAll();

    auto future = promise.get_future();
    ASSERT_EQ(future.wait_until(std::chrono::steady_clock::time_point::min()), std::future_status::ready);
    ASSERT_ANY_THROW(future.get());
}

TEST_F(FDBCoroutineTest, CancelUncancelable)
{
    auto delay = [](std::chrono::system_clock::duration d) -> Task<void>
    {
        co_await fdb_delay(0.1); /// Make following code runing on fdb thread
        std::this_thread::sleep_for(d);
        co_return;
    };

    FoundationDB::AsyncTrxTracker tracker;

    std::promise<void> promise;
    delay(std::chrono::seconds{1})
        .start(
            [&](std::exception_ptr eptr)
            {
                if (eptr)
                    promise.set_exception(eptr);
                else
                    promise.set_value();
            },
            tracker.newToken());

    std::this_thread::sleep_for(std::chrono::milliseconds{500});
    tracker.gracefullyCancelAll();

    auto future = promise.get_future();
    ASSERT_EQ(future.wait_until(std::chrono::steady_clock::time_point::min()), std::future_status::ready);
    ASSERT_NO_THROW(future.get());
}

TEST_F(FDBCoroutineTest, CannotCreateTokenIfTrackerIsCanceled)
{
    FoundationDB::AsyncTrxTracker tracker;
    tracker.gracefullyCancelAll();
    ASSERT_ANY_THROW(tracker.newToken());
}

TEST_F(FDBCoroutineTest, CancelMultiTrx)
{
    FoundationDB::AsyncTrxTracker tracker;

    std::promise<void> promise_finish;
    std::vector<std::promise<void>> promises_cancel;

    auto delay = [](double second) -> Task<void> { co_await fdb_delay(second); };
    auto start_delay = [&](double second, std::promise<void> & promise)
    {
        delay(second).start(
            [&promise](std::exception_ptr eptr)
            {
                if (eptr)
                    promise.set_exception(eptr);
                else
                    promise.set_value();
            },
            tracker.newToken());
    };

    start_delay(0.5, promise_finish);
    promises_cancel.resize(3);
    start_delay(1, promises_cancel[0]);
    start_delay(2, promises_cancel[1]);
    start_delay(3, promises_cancel[2]);

    promise_finish.get_future().wait();
    tracker.gracefullyCancelAll();

    for (auto & promise : promises_cancel)
    {
        auto future = promise.get_future();
        ASSERT_EQ(future.wait_until(std::chrono::steady_clock::time_point::min()), std::future_status::ready);
        ASSERT_ANY_THROW(future.get());
    }
}

TEST_F(FDBCoroutineTest, CancelMultiTime)
{
    auto delay = [](double second) -> Task<void> { co_await fdb_delay(second); };

    FoundationDB::AsyncTrxTracker tracker;

    std::promise<void> promise;
    delay(0.5).start(
        [&](std::exception_ptr eptr)
        {
            if (eptr)
                promise.set_exception(eptr);
            else
                promise.set_value();
        },
        tracker.newToken());

    tracker.gracefullyCancelAll();
    tracker.gracefullyCancelAll();
    tracker.gracefullyCancelAll();

    auto future = promise.get_future();
    ASSERT_EQ(future.wait_until(std::chrono::steady_clock::time_point::min()), std::future_status::ready);
    ASSERT_ANY_THROW(future.get());
}

TEST_F(FDBCoroutineTest, CancelEventNotifier)
{
    auto delay = [](double second) -> Task<void> { co_await fdb_delay(second); };
    FoundationDB::AsyncTrxTracker tracker;

    std::promise<void> promise;
    delay(0.5).start([&](std::exception_ptr) {}, tracker.newToken());

    auto ev_h = EventNotifier::instance().subscribe(Coordination::Error::ZSESSIONEXPIRED, [&]() { promise.set_value(); });

    tracker.gracefullyCancelAll();

    auto future = promise.get_future();
    ASSERT_EQ(future.wait_for(std::chrono::milliseconds{500}), std::future_status::ready);
}
}
#endif
