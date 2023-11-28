#pragma once

#include <cstddef>
#include <exception>
#include <memory>
#include <vector>
#include <fmt/format.h>
#include <foundationdb/fdb_c.h>
#include <Poco/Logger.h>
#include <Common/Exception.h>
#include <Common/FoundationDB/FoundationDBCommon.h>
#include <Common/FoundationDB/FoundationDBHelpers.h>
#include <Common/ThreadPool.h>
#include <Common/logger_useful.h>

#include "AsyncTrxTracker.h"
#include "FDBExtended.h"

// #define FDB_ASYNC_TRX_ENABLE_THREAD_POOL
// #define ASYNC_TRX_DEBUG_LOG

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
}

namespace DB::FoundationDB
{

/// Local Async RWLock
struct AsyncTrxRWLock
{
    AsyncTrxRWLock() = default;
    AsyncTrxRWLock & operator=(AsyncTrxRWLock && other) noexcept
    {
        std::swap(holder, other.holder);
#ifdef ASYNC_TRX_DEBUG_LOG
        std::swap(locked_path, other.locked_path);
#endif
        return *this;
    }
    ~AsyncTrxRWLock()
    {
        if (holder)
        {
#ifdef ASYNC_TRX_DEBUG_LOG
            LOG_DEBUG(log, "unlock {} {}", static_cast<void *>(holder), fmt::join(locked_path, ","));
#endif
            fdb_rwlocks_free(holder);
        }
    }

    // NOLINTNEXTLINE(readability-make-member-function-const)
    FDBFuture * lock()
    {
        if (holder)
        {
#ifdef ASYNC_TRX_DEBUG_LOG
            LOG_DEBUG(log, "lock {} {}", static_cast<void *>(holder), fmt::join(locked_path, ","));
#endif
            return fdb_rwlocks_lock(holder);
        }
        else
        {
            return nullptr;
        }
    }

    void shared(const std::string & key)
    {
        if (!holder)
            holder = fdb_rwlocks_create();
        fdb_rwlocks_shared(holder, key.data());

#ifdef ASYNC_TRX_DEBUG_LOG
        locked_path.emplace_back(key);
#endif
    }

    void exclusive(const std::string & key)
    {
        if (!holder)
            holder = fdb_rwlocks_create();
        fdb_rwlocks_exclusive(holder, key.data());

#ifdef ASYNC_TRX_DEBUG_LOG
        locked_path.emplace_back(key + "*");
#endif
    }

    FDBRWLockHodler * holder = nullptr;

#ifdef ASYNC_TRX_DEBUG_LOG
    Poco::Logger * log = &Poco::Logger::get("FDBAsyncTrxRWLock");
    std::vector<std::string> locked_path;
#endif
};

/// Context of transaction coroutine
///
/// * Current step
/// * Variables
/// * Lock
/// * Callback: Callback will be called exactly once at the end of the transaction.
/// * CancelToken
///
class AsyncTrxContext
{
public:
    using Callback = std::function<void(AsyncTrxContext &, std::exception_ptr)>;

    AsyncTrxContext() = default;
    explicit AsyncTrxContext(FDBTransaction * tr_, Callback callback) : tr(tr_), on_finially(std::move(callback)), finished(false) { }

    AsyncTrxContext & operator=(AsyncTrxContext && other) noexcept
    {
        std::swap(tr, other.tr);
        std::swap(vars_buf, other.vars_buf);
        std::swap(vars_buf_size, other.vars_buf_size);
        std::swap(vars_ctrls, other.vars_ctrls);
        std::swap(on_finially, other.on_finially);
        std::swap(finished, other.finished);
        std::swap(trackerToken, other.trackerToken);
        lock = std::move(other.lock);
        return *this;
    }

    ~AsyncTrxContext()
    {
        if (!finished)
            finish(std::make_exception_ptr(
                Exception(ErrorCodes::LOGICAL_ERROR, "Unreachable code. It's possible that finish() was forgotten to be called.")));

        if (tr != nullptr)
            fdb_transaction_destroy(tr);
        if (vars_buf != nullptr)
        {
            destoryVars();
            free(vars_buf);
        }
    }

    /// Get FDBTransacion object
    auto getTrx() const { return tr; }

    /// Repeat current step
    void repeat()
    {
        /// Let the next step (++tick) remain itself.
        --tick;
    }

    /// Goto step (from current step)
    void gotoCur(int64_t offset) { tick += offset - 1; }

    /// Goto step (from start)
    void gotoSet(size_t offset) { tick = offset; }

    void reset()
    {
        fdb_transaction_reset(tr);
        destoryVars();
        initVars();
        tick = 0;
        finished = false;
    }

    /// Index number of next step
    size_t tick = 0;

    /// Lock
    AsyncTrxRWLock lock;

    /// CancelToken
    std::unique_ptr<AsyncTrxCancelToken> trackerToken;

    void finish(std::exception_ptr eptr = nullptr)
    {
        if (on_finially)
        {
            finished = true;
            try
            {
                on_finially(*this, eptr);
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__, "Uncaught exception in trx callback");
            }
        }
    }

private:
    FDBTransaction * tr = nullptr;

    Callback on_finially;
    bool finished = true;

public:
    template <typename T>
    struct VarDesc
    {
        size_t offset;

        template <typename U>
        const VarDesc<U> & as() const
        {
            return reinterpret_cast<const VarDesc<U> &>(*this);
        }
    };

    struct VarCtrl
    {
        using CtorFn = std::function<void(void *)>;
        using DtorFn = std::function<void(void *)>;

        CtorFn ctorFn;
        DtorFn dtorFn;

        void ctor(void * buf) const
        {
            if (ctorFn)
                ctorFn(buf);
        }

        void dtor(void * buf) const
        {
            if (dtorFn)
                dtorFn(buf);
        }
    };

    /// Get variable in context
    template <typename T>
    T * getVar(VarDesc<T> desc)
    {
        return reinterpret_cast<T *>(vars_buf + desc.offset);
    }

    /// Construct all vars in context
    void initVars()
    {
        if (vars_buf)
            return;

        vars_buf = reinterpret_cast<char *>(malloc(vars_buf_size));
        for (auto & [offset, ctrl] : vars_ctrls)
            ctrl.ctor(vars_buf + offset);
    }

    /// Deconstruct all (non-trivial) vars in context.
    void destoryVars()
    {
        if (!vars_buf)
            return;

        for (auto & [offset, ctrl] : vars_ctrls)
            ctrl.dtor(vars_buf + offset);
        free(vars_buf);
        vars_buf = nullptr;
    }

private:
    char * vars_buf = nullptr;
    size_t vars_buf_size = 0;
    std::vector<std::pair<size_t, VarCtrl>> vars_ctrls;

    friend class AsyncTrxBuilder;
};

/// AsyncTrx is a simple generator-based coroutine for FDBTranscation.
///
/// Equivalent to:
///   try {
///       FDBFuture *f = nullptr;
///       for (step in steps) {
///           f = step(f)
///           wait(f);
///       }
///   } catch (...) {
///       onError();
///   }

class AsyncTrx
{
public:
    using Context = AsyncTrxContext;
    using Ptr = std::shared_ptr<AsyncTrx>;

    /// Async steps in a transaction. It take the Future of last step and
    /// return new Future.
    using Step = std::function<FDBFuture *(Context &, FDBFuture *)>;

    using Callback = std::function<void(Context &, std::exception_ptr)>;

    explicit AsyncTrx(std::vector<Step> && steps_) : steps(std::move(steps_))
    {
#ifdef ASYNC_TRX_DEBUG_LOG
        LOG_DEBUG(log, "New trx: {}", static_cast<void *>(this));
#endif
    }

    ~AsyncTrx()
    {
#ifdef ASYNC_TRX_DEBUG_LOG
        LOG_DEBUG(log, "Destroy trx: {}", static_cast<void *>(this));
#endif
    }

    /// Generate next Future. If the return value is null, the iterator is finished.
    FDBFuture * next(FDBFuture * last_future)
    {
        FDBFuture * future = nullptr;

        if (ctx.trackerToken)
            ctx.trackerToken->assertNotCanceled();

        if (ctx.tick >= steps.size())
            future = nullptr;

        while (ctx.tick < steps.size())
        {
            /// Run current step
            auto & step = steps[ctx.tick++];
            future = step(ctx, last_future);

            /// If current step return a Future, yield Future.
            if (future != nullptr)
                break;
            /// Otherwise, execute next step immediately.
            else
                last_future = nullptr;
        }

        if (ctx.trackerToken)
            ctx.trackerToken->setCancelPoint(future);
        return future;
    }

    /// Start coroutine in background. It will take the ownership of `g` and
    /// free on finished.
    static void startInBackground(Ptr trx)
    {
        auto * lock_future = trx->ctx.lock.lock();
        trx->ctx.initVars();
        if (lock_future)
        {
            throwIfFDBError(fdb_future_set_callback(lock_future, onFutureReady, new Ptr(trx)));
        }
        else
        {
            onFutureReady(nullptr, new Ptr(trx));
        }
    }

    AsyncTrxContext ctx;

private:
    std::vector<Step> steps;

#ifdef ASYNC_TRX_DEBUG_LOG
    Poco::Logger * log = &Poco::Logger::get("AsyncTrx");
#endif

    /// Handle future produced by trx->next(). Invoke next() or abort() if finished.
    static void onFutureReady(FDBFuture * f, void * payload) noexcept
    {
        auto & trx = *static_cast<Ptr *>(payload);

#ifdef FDB_ASYNC_TRX_ENABLE_THREAD_POOL
        try
        {
            GlobalThreadPool::instance().scheduleOrThrow([trx, f]()
            {
#endif
                try
                {
                    if (auto * next_future = trx->next(f))
                        throwIfFDBError(fdb_future_set_callback(next_future, onFutureReady, new Ptr(trx)));
                    else
                        trx->ctx.finish();
                }
                catch (FoundationDBException & e)
                {
                    auto * on_error_future = fdb_transaction_on_error(trx->ctx.getTrx(), e.code);
                    try
                    {
                        throwIfFDBError(fdb_future_set_callback(on_error_future, onFutureOnError, new Ptr(trx)));
                    }
                    catch (...)
                    {
                        trx->ctx.finish(std::current_exception());
                    }
                }
                catch (...)
                {
                    trx->ctx.finish(std::current_exception());
                }

                if (f)
                    fdb_future_destroy(f);
#ifdef FDB_ASYNC_TRX_ENABLE_THREAD_POOL
            });
        }
        catch (...)
        {
            trx->abort(std::current_exception());
            if (f)
                fdb_future_destroy(f);
        }
#endif

        delete &trx;
    }

    /// Handle fdb_transaction_on_error(). Restart transaction if possible.
    static void onFutureOnError(FDBFuture * f, void * payload) noexcept
    {
        assert(f != nullptr);
        auto & trx = *static_cast<Ptr *>(payload);

        try
        {
            throwIfFDBError(fdb_future_get_error(f));

            /// Retry transaction
            trx->ctx.destoryVars();
            trx->ctx.initVars();
            trx->ctx.gotoSet(0);
            onFutureReady(nullptr, new Ptr(trx));
        }
        catch (...)
        {
            trx->ctx.finish(std::current_exception());
        }

        fdb_future_destroy(f);
        delete &trx;
    }

    friend class AsyncTrxBuilder;
};

/// Transaction Builder.
/// Bundle a series of async steps to a single coroutine dynamically.
/// AsyncTrx's performance is not as good as the Actor in fdb, but it is easy to
/// implement and does not depend on the flow language.

class AsyncTrxBuilder
{
public:
    using Context = AsyncTrxContext;
    template <typename T>
    using VarDesc = Context::VarDesc<T>;
    using VarCtrl = Context::VarCtrl;
    using Step = AsyncTrx::Step;

    AsyncTrxBuilder() = default;
    AsyncTrxBuilder(AsyncTrxBuilder && other) noexcept { steps.swap(other.steps); }

    /// Append a async step
    template <typename TStep>
    AsyncTrxBuilder & then(TStep && fn)
    {
        steps.emplace_back(std::forward<TStep>(fn));
        return *this;
    }

    /// Not recommended, due to the additional memory overhead.
    AsyncTrxBuilder & then(AsyncTrxBuilder && other)
    {
        std::move(other.steps.begin(), other.steps.end(), std::back_inserter(steps));
        other.steps.clear();
        return *this;
    }

    size_t nextStepTick() const { return steps.size(); }

    /// Commit transaction
    AsyncTrxBuilder & commit()
    {
        steps.emplace_back([](Context & ctx, FDBFuture *) { return fdb_transaction_commit(ctx.getTrx()); });
        steps.emplace_back([](Context &, FDBFuture * f) -> FDBFuture * {
            throwIfFDBError(fdb_future_get_error(f));
            return nullptr;
        });
        return *this;
    }

    /// Commit transaction and get versionstamp
    AsyncTrxBuilder & commit(VarDesc<FDBVersionstamp> var_versionstamp)
    {
        using FDBFuturePtr = std::unique_ptr<FDBFuture, void (*)(FDBFuture *)>;
        auto var_future = var<FDBFuturePtr>(nullptr, &fdb_future_destroy);

        steps.emplace_back(
            [var_future](Context & ctx, FDBFuture *)
            {
                auto & future = *ctx.getVar(var_future);

                future = FDBFuturePtr(fdb_transaction_get_versionstamp(ctx.getTrx()), fdb_future_destroy);
                return nullptr;
            });
        commit();
        steps.emplace_back([var_future](Context & ctx, FDBFuture *) { return ctx.getVar(var_future)->release(); });
        steps.emplace_back(
            [var_versionstamp](Context & ctx, FDBFuture * f)
            {
                auto & vs = *ctx.getVar(var_versionstamp);

                const uint8_t * vs_bytes;
                int vs_len;

                throwIfFDBError(fdb_future_get_key(f, &vs_bytes, &vs_len));
                assert(vs_len == 10);

                memcpy(&vs.bytes, vs_bytes, 10);
                return nullptr;
            });

        return *this;
    }

    /// Alloc a variable in transaction context. Non-trivial variable will be init by default constructor.
    template <typename T, typename... Args>
    VarDesc<T> var(Args &&... args)
    {
        total_vars_size += (alignof(T) - total_vars_size % alignof(T)) % alignof(T);
        VarDesc<T> desc = {total_vars_size};
        total_vars_size += sizeof(T);

        // new T(std::forward<Args>(args)...);
        if constexpr (!std::is_trivial_v<T>)
        {
            VarCtrl ctrl;
            ctrl.ctorFn
                = [... lambda_args = std::forward<Args>(args)](void * buf) mutable { new (buf) T(std::forward<Args>(lambda_args)...); };
            if (!std::is_trivially_destructible_v<T>)
                ctrl.dtorFn = [](void * buf) { reinterpret_cast<T *>(buf)->~T(); };
            vars_ctrls.emplace_back(desc.offset, ctrl);
        }

        return desc;
    }

    /// Alloc a variable in transaction context and set default variable.
    template <typename T, typename Default>
    std::enable_if_t<std::is_trivially_assignable_v<T &, Default>, VarDesc<T>> varDefault(Default && default_val)
    {
        total_vars_size += (alignof(T) - total_vars_size % alignof(T)) % alignof(T);
        VarDesc<T> desc{total_vars_size};
        total_vars_size += sizeof(T);

        VarCtrl ctrl;
        ctrl.ctorFn
            = [lambda_default_val = std::forward<Default>(default_val)](void * buf) { *reinterpret_cast<T *>(buf) = lambda_default_val; };
        if (!std::is_trivially_destructible_v<T>)
            ctrl.dtorFn = [](void * buf) { reinterpret_cast<T *>(buf)->~T(); };
        vars_ctrls.emplace_back(desc.offset, ctrl);

        return desc;
    }

    // AsyncTrx provides local 2 phase locking (2PL). AsyncTrx's 2PL is
    // conservative. It locks before the transaction starts and releases after
    // the transaction ends. Locks are not released on transaction retries.
    // So you can add a lock anywhere in the transaction, but it won't cause a
    // deadlock. The 2PL here are primarily intended to achieve real-time order
    // transaction within a single process.

    // Acquire shared lock on key. The order is not important. Locks will be added before the transaction starts.
    void lockShared(const std::string & key) { lock.shared(key); }

    // Acquire exclusive lock on key. The order is not important. Locks will be added before the transaction starts.
    void lockExclusive(const std::string & key) { lock.exclusive(key); }

    /// build() will take the ownership of the transaction.
    template <typename Callback>
    AsyncTrx::Ptr build(FDBTransaction * tr, Callback callback, std::unique_ptr<AsyncTrxCancelToken> token = nullptr)
    {
        auto trx = std::make_shared<AsyncTrx>(std::move(steps));
        trx->ctx = Context(tr, callback);
        trx->ctx.lock = std::move(lock);
        if (token)
            trx->ctx.trackerToken = std::move(token);
        trx->ctx.vars_buf_size = total_vars_size;
        trx->ctx.vars_ctrls = std::move(vars_ctrls);

        return trx;
    }

    /// Set callback and exec the transaction.
    /// exec() will take the ownership of the transaction.
    template <typename Callback>
    void exec(FDBTransaction * tr, Callback callback, std::unique_ptr<AsyncTrxCancelToken> trackerToken = nullptr)
    {
        auto trx = build(tr, callback, std::move(trackerToken));
        AsyncTrx::startInBackground(trx);
    }

private:
    std::vector<Step> steps;
    size_t total_vars_size = 0;
    std::vector<std::pair<size_t, VarCtrl>> vars_ctrls;
    AsyncTrxRWLock lock;
};

template <typename T>
using AsyncTrxVar = AsyncTrxContext::VarDesc<T>;
}
