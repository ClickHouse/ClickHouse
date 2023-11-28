#include "config.h"

#if USE_FDB

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <future>
#include <memory>
#include <string>
#include <type_traits>
#include <vector>
#include <base/defines.h>
#include <foundationdb/fdb_c.h>
#include <gtest/gtest.h>
#include <Poco/AutoPtr.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/EventNotifier.h>
#include <Common/Exception.h>
#include <Common/FoundationDB/FoundationDBCommon.h>
#include <Common/FoundationDB/FoundationDBHelpers.h>
#include <Common/FoundationDB/internal/AsyncTrx.h>
#include <Common/FoundationDB/internal/AsyncTrxTracker.h>
#include <Common/FoundationDB/tests/gtest_fdb_common.h>
#include <Common/ZooKeeper/IKeeper.h>

using namespace DB;
namespace fdb = DB::FoundationDB;

/// Operators and macros to build a AsyncTrx for test

namespace DB::FoundationDB
{
template <typename Thenable>
AsyncTrxBuilder & operator|(AsyncTrxBuilder & trx, Thenable && fn)
{
    return trx.then(std::forward<Thenable>(fn));
}

template <typename Thenable>
AsyncTrxBuilder && operator|(AsyncTrxBuilder && trx, Thenable && fn)
{
    trx.then(std::forward<Thenable>(fn));
    return std::move(trx);
}
}

#ifdef TRX_STEP
#    undef TRX_STEP
#endif
#define TRX_STEP(...) [__VA_ARGS__]([[maybe_unused]] fdb::AsyncTrx::Context & ctx, [[maybe_unused]] FDBFuture * f)
#define TRX_CALLBACK(...) [__VA_ARGS__]([[maybe_unused]] fdb::AsyncTrx::Context & ctx, std::exception_ptr eptr)
#define START_TRX ::DB::FoundationDB::AsyncTrxBuilder()

class FDBAsyncTrx : public FDBFixture
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

    void set(const std::string & key, const std::string & value)
    {
        auto tr = fdb_manage_object(newTrx());

        fdb_transaction_set(tr.get(), FDB_KEY_FROM_STRING(key), FDB_VALUE_FROM_STRING(value));

        auto f = fdb_manage_object(fdb_transaction_commit(tr.get()));
        throwIfFDBError(fdb_future_block_until_ready(f.get()));
        throwIfFDBError(fdb_future_get_error(f.get()));
    }

    static std::string key(const std::string & key) { return std::string(key_prefix) + key; }

    template <typename R>
    R wait(std::promise<R> & promise)
    {
        auto future = promise.get_future();
        future.wait();
        return future.get();
    }

    template <int * out_state>
    struct MockTrivialAssignable
    {
        enum Flags : uint8_t
        {
            DefaultConstructor,
            Deconstructor
        };

        MockTrivialAssignable() { flag(DefaultConstructor); }
        explicit MockTrivialAssignable(int i_) : i(i_) { }
        ~MockTrivialAssignable() { flag(Deconstructor); }

        MockTrivialAssignable(const MockTrivialAssignable<out_state> &) = default;
        MockTrivialAssignable<out_state> & operator=(const MockTrivialAssignable<out_state> &) = default;
        MockTrivialAssignable(MockTrivialAssignable<out_state> &&) noexcept = default;
        MockTrivialAssignable<out_state> & operator=(MockTrivialAssignable<out_state> &&) noexcept = default;

        void flag(Flags flag) { *out_state |= (1 << flag); }
        static bool called(Flags flag) { return (*out_state & (1 << flag)) == (1 << flag); }

        int i = 0;
    };
};

void printUsage()
{
    std::cerr << "Usage: fdb_async_trx [--config config_file] [-h|--help] [...gtest args]" << std::endl;
}

TEST_F(FDBAsyncTrx, GetFutureAndLoop)
{
    std::promise<bool> promise_success;
    std::promise<bool> promise_key_exists;
    std::promise<size_t> promise_loop_cnt;

    auto key_a = key("a");

    {
        auto loop = std::make_shared<size_t>(0);
        auto trx = START_TRX | TRX_STEP(key_a, log = log)
        {
            LOG_TRACE(log, "get key: {}", fdb_print_key(key_a));
            return fdb_transaction_get(ctx.getTrx(), FDB_KEY_FROM_STRING(key_a), true);
        }
        | TRX_STEP(&promise_key_exists, log = log)
        {
            fdb_bool_t present;
            const uint8_t * value;
            int len;
            throwIfFDBError(fdb_future_get_value(f, &present, &value, &len));

            LOG_TRACE(log, "key is present: {}", present);
            promise_key_exists.set_value(present);

            return nullptr;
        }
        | TRX_STEP(loop, log = log)
        {
            LOG_TRACE(log, "loop: {}", *loop);
            if (++*loop < 3)
                ctx.repeat();

            return nullptr;
        }
        | TRX_STEP(loop, &promise_loop_cnt)
        {
            promise_loop_cnt.set_value(*loop);

            return nullptr;
        };

        trx.exec(
            newTrx(), TRX_CALLBACK(&) { promise_success.set_value(!eptr); });
    }

    ASSERT_TRUE(wait(promise_success));
    ASSERT_FALSE(wait(promise_key_exists));
    ASSERT_EQ(wait(promise_loop_cnt), 3);
}

TEST_F(FDBAsyncTrx, LoopFuture)
{
    std::promise<bool> promise_success;
    std::promise<std::vector<std::string>> promise_list;

    auto key_prefix = key("list/");
    for (char c = 'a'; c <= 'z'; c++)
    {
        set(key_prefix + c, std::string() + c);
    }

    {
        auto list = std::make_shared<std::vector<std::string>>();
        auto trx = START_TRX | TRX_STEP(n = 0, key_prefix, list, log = log) mutable->FDBFuture *
        {
            LOG_TRACE(log, "loop {}", (n++));
            std::string range_start;

            if (f != nullptr)
            {
                const FDBKeyValue * kvs;
                int cnt;
                fdb_bool_t more;
                throwIfFDBError(fdb_future_get_keyvalue_array(f, &kvs, &cnt, &more));

                LOG_TRACE(log, "{} {}", cnt, more);

                for (int i = 0; i < cnt; i++)
                {
                    const auto & kv = kvs[i];
                    list->emplace_back(reinterpret_cast<const char *>(kv.value), kv.value_length);
                }

                range_start = std::string(reinterpret_cast<const char *>(kvs[cnt - 1].key), kvs[cnt - 1].key_length);

                if (!more)
                    return nullptr;
            }
            else
            {
                range_start = key_prefix;
            }

            ctx.repeat();

            auto range_end = key_prefix + '\xff';
            LOG_TRACE(log, "{}..{}", fdb_print_key(range_start), fdb_print_key(range_end));
            return fdb_transaction_get_range(
                ctx.getTrx(),
                FDB_KEYSEL_FIRST_GREATER_THAN_STRING(range_start),
                FDB_KEYSEL_FIRST_GREATER_OR_EQUAL_STRING(range_end),
                20,
                0,
                FDB_STREAMING_MODE_EXACT,
                0,
                1,
                0);
        };

        trx.exec(
            newTrx(), TRX_CALLBACK(&, list) {
                promise_success.set_value(!eptr);
                if (!eptr)
                    promise_list.set_value(std::move(*list));
            });
    }

    ASSERT_TRUE(wait(promise_success));
    ASSERT_EQ(wait(promise_list).size(), 26);
}

TEST_F(FDBAsyncTrx, VariableShouldWork)
{
    std::promise<void> promise;

    auto trx = START_TRX;

    auto i_h = trx.var<int>();
    auto v_h = trx.var<std::vector<int>>();

    trx | TRX_STEP(i_h)
    {
        auto & i = *ctx.getVar(i_h);
        i = 100;
        return nullptr;
    }
    | TRX_STEP(v_h)
    {
        auto & v = *ctx.getVar(v_h);
        v.emplace_back(200);
        return nullptr;
    };

    int i;
    std::vector<int> v;
    trx.exec(
        newTrx(), TRX_CALLBACK(&) {
            if (eptr)
            {
                promise.set_exception(std::current_exception());
            }
            else
            {
                i = *ctx.getVar(i_h);
                v = *ctx.getVar(v_h);
                promise.set_value();
            }
        });

    wait(promise);
    ASSERT_EQ(i, 100);
    ASSERT_EQ(v[0], 200);
}

TEST_F(FDBAsyncTrx, VariableDefaultValue)
{
    std::promise<int> promise;

    auto trx = START_TRX;
    auto i_h = trx.varDefault<int>(123);

    trx.exec(
        newTrx(), TRX_CALLBACK(&) {
            if (eptr)
                promise.set_exception(eptr);
            else
                promise.set_value(*ctx.getVar(i_h));
        });

    ASSERT_EQ(wait(promise), 123);
}

TEST_F(FDBAsyncTrx, TrivialAssignableVariableShouldSkipConstructor)
{
    static int state;
    using Mock = MockTrivialAssignable<&state>;

    std::promise<void> promise;

    auto trx = START_TRX;
    [[maybe_unused]] auto var_mock = trx.varDefault<Mock>(Mock(1));

    state = 0;
    trx.exec(
        newTrx(), TRX_CALLBACK(&) {
            if (eptr)
                promise.set_exception(eptr);
            else
                promise.set_value();
        });
    wait(promise);

    EXPECT_FALSE(Mock::called(Mock::DefaultConstructor));
    EXPECT_TRUE(Mock::called(Mock::Deconstructor));
}

TEST_F(FDBAsyncTrx, VariableShouldConstructorWhenPreviousStepFailed)
{
    static int state;
    using Mock = MockTrivialAssignable<&state>;

    auto trx = START_TRX | TRX_STEP()->FDBFuture *
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "error");
    };
    [[maybe_unused]] auto var_mock = trx.var<Mock>();

    state = 0;
    std::promise<void> promise;
    trx.exec(
        newTrx(), TRX_CALLBACK(&) {
            if (eptr)
                promise.set_exception(eptr);
            else
                promise.set_value();
        });

    ASSERT_THROW(wait(promise), Exception);
    EXPECT_TRUE(Mock::called(Mock::DefaultConstructor));
    EXPECT_TRUE(Mock::called(Mock::Deconstructor));
}

TEST_F(FDBAsyncTrx, ThrowException)
{
    std::string key_a = key("a");
    std::promise<void> promise;

    auto trx = START_TRX | TRX_STEP(key_a)
    {
        return fdb_transaction_get(ctx.getTrx(), FDB_KEY_FROM_STRING(key_a), true);
    }
    | TRX_STEP()->FDBFuture *
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "bad");
    }
    | TRX_STEP(key_a)
    {
        return fdb_transaction_get(ctx.getTrx(), FDB_KEY_FROM_STRING(key_a), true);
    };

    trx.exec(
        newTrx(), TRX_CALLBACK(&) {
            if (eptr)
                promise.set_exception(eptr);
            else
                promise.set_value();
        });

    EXPECT_THROW(wait(promise), Exception);
}

TEST_F(FDBAsyncTrx, Delay)
{
    using namespace std::chrono;
    std::promise<void> promise;

    auto trx = START_TRX | TRX_STEP()
    {
        return fdb_delay(0.5);
    };

    auto start = system_clock::now();
    trx.exec(
        newTrx(), TRX_CALLBACK(&) {
            if (eptr)
                promise.set_exception(eptr);
            else
                promise.set_value();
        });

    wait(promise);
    auto end = system_clock::now();

    EXPECT_TRUE(end - start > 400ms);
    EXPECT_TRUE(end - start < 600ms);
}

TEST_F(FDBAsyncTrx, LockShouldWork)
{
    using namespace std::chrono;

    std::vector<fdb::AsyncTrxBuilder> trxs;
    trxs.reserve(3);
    for (int i = 0; i < 3; i++)
    {
        trxs.emplace_back(START_TRX | TRX_STEP(i, log = log) {
            LOG_TRACE(log, "begin trx {}", i);
            return fdb_delay(0.5);
        });
    }

    trxs[0].lockShared("a");
    trxs[0].lockShared("b");

    trxs[1].lockShared("b");

    trxs[2].lockShared("a");
    trxs[2].lockExclusive("b");

    auto start = system_clock::now();
    std::vector<std::promise<system_clock::time_point>> promises;
    for (auto & trx : trxs)
    {
        promises.emplace_back();
        trx.exec(
            newTrx(), TRX_CALLBACK(&, i = promises.size() - 1) {
                LOG_TRACE(log, "end trx {}", i);
                if (eptr)
                    promises[i].set_exception(eptr);
                else
                    promises[i].set_value(system_clock::now());
            });
    }

    ASSERT_LT(wait(promises[0]), start + 1s);
    ASSERT_LT(wait(promises[1]), start + 1s);
    ASSERT_GT(wait(promises[2]), start + 1s);
}

TEST_F(FDBAsyncTrx, TSanLogInTrx)
{
    std::promise<bool> promise_success;
    auto * log = &Poco::Logger::get("GTestFDBAsyncTrx");

    {
        auto trx = START_TRX | TRX_STEP()
        {
            return fdb_delay(0.5);
        }
        | TRX_STEP(log_ = log)
        {
            LOG_DEBUG(log_, "log");
            return nullptr;
        };

        trx.exec(
            newTrx(), TRX_CALLBACK(&) { promise_success.set_value(!eptr); });
    }

    ASSERT_TRUE(wait(promise_success));
}

TEST_F(FDBAsyncTrx, CancelBlockingTrx)
{
    FoundationDB::AsyncTrxTracker tracker;

    std::promise<void> promise;
    auto trx = START_TRX | TRX_STEP()
    {
        return fdb_delay(0.5);
    };
    trx.exec(
        newTrx(),
        TRX_CALLBACK(&) {
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

TEST_F(FDBAsyncTrx, CancelUncancelableTrx)
{
    FoundationDB::AsyncTrxTracker tracker;

    std::promise<void> promise;
    auto trx = START_TRX | TRX_STEP()
    {
        std::this_thread::sleep_for(std::chrono::milliseconds{500});
        return nullptr;
    };
    trx.exec(
        newTrx(),
        TRX_CALLBACK(&) {
            if (eptr)
                promise.set_exception(eptr);
            else
                promise.set_value();
        },
        tracker.newToken());

    tracker.gracefullyCancelAll();

    auto future = promise.get_future();
    ASSERT_EQ(future.wait_until(std::chrono::steady_clock::time_point::min()), std::future_status::ready);
    ASSERT_NO_THROW(future.get());
}

TEST_F(FDBAsyncTrx, TrxCannotStartIfTrackerIsCanceled)
{
    FoundationDB::AsyncTrxTracker tracker;

    auto trx = START_TRX | TRX_STEP()
    {
        return fdb_delay(0.5);
    };

    tracker.gracefullyCancelAll();

    // FIXME: newTrx() may leak because tracker.newToken() is failed
    // ASSERT_ANY_THROW(trx.exec(
    //     newTrx(), TRX_CALLBACK(&) { UNUSED(eptr); }, tracker.newToken()));
    ASSERT_ANY_THROW(tracker.newToken());
}

TEST_F(FDBAsyncTrx, CancelMultiTrx)
{
    FoundationDB::AsyncTrxTracker tracker;

    std::promise<void> promise_finish;
    std::vector<std::promise<void>> promises_cancel;

    auto startDelayTrx = [&](double delay, std::promise<void> & promise)
    {
        auto trx = START_TRX | TRX_STEP(delay)
        {
            return fdb_delay(delay);
        };

        trx.exec(
            newTrx(),
            TRX_CALLBACK(&) {
                if (eptr)
                    promise.set_exception(eptr);
                else
                    promise.set_value();
            },
            tracker.newToken());
    };

    startDelayTrx(0.5, promise_finish);
    promises_cancel.resize(3);
    startDelayTrx(1, promises_cancel[0]);
    startDelayTrx(2, promises_cancel[1]);
    startDelayTrx(3, promises_cancel[2]);

    wait(promise_finish);
    tracker.gracefullyCancelAll();

    for (auto & promise : promises_cancel)
    {
        auto future = promise.get_future();
        ASSERT_EQ(future.wait_until(std::chrono::steady_clock::time_point::min()), std::future_status::ready);
        ASSERT_ANY_THROW(future.get());
    }
}

TEST_F(FDBAsyncTrx, CancelMultiTime)
{
    FoundationDB::AsyncTrxTracker tracker;

    std::promise<void> promise;
    auto trx = START_TRX | TRX_STEP()
    {
        return fdb_delay(0.5);
    };
    trx.exec(
        newTrx(),
        TRX_CALLBACK(&) {
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

TEST_F(FDBAsyncTrx, CancelEventNotifier)
{
    FoundationDB::AsyncTrxTracker tracker;

    std::promise<void> promise;
    auto trx = START_TRX | TRX_STEP()
    {
        return fdb_delay(0.5);
    };
    trx.exec(
        newTrx(), TRX_CALLBACK() { UNUSED(eptr); }, tracker.newToken());

    auto ev_h = EventNotifier::instance().subscribe(Coordination::Error::ZSESSIONEXPIRED, [&]() { promise.set_value(); });

    tracker.gracefullyCancelAll();

    auto future = promise.get_future();
    ASSERT_EQ(future.wait_for(std::chrono::milliseconds{500}), std::future_status::ready);
}

TEST_F(FDBAsyncTrx, CancelBlockingTrxWithCancelSource)
{
    FoundationDB::AsyncTrxCancelSource cancel_source;

    std::promise<void> promise;
    auto trx = START_TRX | TRX_STEP()
    {
        return fdb_delay(0.5);
    };
    trx.exec(
        newTrx(),
        TRX_CALLBACK(&) {
            if (eptr)
                promise.set_exception(eptr);
            else
                promise.set_value();
        },
        cancel_source.getToken());

    cancel_source.cancel();

    auto future = promise.get_future();
    ASSERT_EQ(future.wait_until(std::chrono::steady_clock::time_point::min()), std::future_status::ready);
    ASSERT_ANY_THROW(future.get());
}

TEST_F(FDBAsyncTrx, CancelUncancelableTrxWithCancelSource)
{
    FoundationDB::AsyncTrxCancelSource cancel_source;

    std::promise<void> promise;
    auto trx = START_TRX | TRX_STEP()
    {
        std::this_thread::sleep_for(std::chrono::milliseconds{500});
        return nullptr;
    };
    trx.exec(
        newTrx(),
        TRX_CALLBACK(&) {
            if (eptr)
                promise.set_exception(eptr);
            else
                promise.set_value();
        },
        cancel_source.getToken());

    cancel_source.cancel();

    auto future = promise.get_future();
    ASSERT_EQ(future.wait_until(std::chrono::steady_clock::time_point::min()), std::future_status::ready);
    ASSERT_NO_THROW(future.get());
}
#endif
