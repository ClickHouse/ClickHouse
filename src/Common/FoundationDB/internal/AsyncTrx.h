#pragma once

#include <exception>
#include <memory>
#include <foundationdb/fdb_c.h>
#include <Common/ThreadPool.h>
#include <Common/Exception.h>
#include <Common/FoundationDB/FoundationDBCommon.h>
#include <Common/FoundationDB/FoundationDBHelpers.h>

// #define FDB_ASYNC_TRX_ENABLE_THREAD_POOL

namespace DB::FoundationDB
{

/// Context of transaction coroutine
class AsyncTrxContext
{
public:
    AsyncTrxContext() = default;
    explicit AsyncTrxContext(FDBTransaction * tr_, size_t buf_size_) : tr(tr_), vars_buf_size(buf_size_)
    {
        if (vars_buf_size > 0)
            vars_buf = reinterpret_cast<char *>(malloc(vars_buf_size));
    }

    AsyncTrxContext & operator=(AsyncTrxContext && other) noexcept
    {
        std::swap(tr, other.tr);
        std::swap(vars_buf, other.vars_buf);
        std::swap(vars_buf_size, other.vars_buf_size);
        return *this;
    }

    ~AsyncTrxContext()
    {
        if (tr != nullptr)
            fdb_transaction_destroy(tr);
        if (vars_buf != nullptr)
        {
            deconstructVars();
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
        deconstructVars();
        tick = 0;
    }

    /// Index number of next step
    size_t tick = 0;

private:
    FDBTransaction * tr = nullptr;

public:
    template <typename T>
    struct VarDesc
    {
        size_t offset;

        static void destory(void * ptr) { reinterpret_cast<T *>(ptr)->~T(); }

        template <typename U>
        const VarDesc<U> & as() const
        {
            return reinterpret_cast<const VarDesc<U> &>(*this);
        }
    };

    /// Get variable in context
    template <typename T>
    T * getVar(VarDesc<T> desc)
    {
        return reinterpret_cast<T *>(vars_buf + desc.offset);
    }

    /// Deconstruct all (non-trivial) vars in context.
    void deconstructVars()
    {
        for (auto & [offset, destroy] : vars_destories)
            destroy(vars_buf + offset);
        vars_destories.clear();
    }

    /// Notify context that the variable need to be deconstruct if needed.
    template <typename T>
    void registerVar(VarDesc<T> desc)
    {
        if constexpr (!std::is_trivially_destructible_v<T>)
            vars_destories.emplace_back(desc.offset, VarDesc<T>::destory);
    }

private:
    char * vars_buf = nullptr;
    size_t vars_buf_size = 0;
    std::vector<std::pair<size_t, void (*)(void *)>> vars_destories;
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

    AsyncTrx(std::vector<Step> && steps_, Callback on_finially_) : steps(std::move(steps_)), on_finially(on_finially_) { }

    /// Generate next Future. If the return value is null, the iterator is finished.
    FDBFuture * next(FDBFuture * last_future)
    {
        FDBFuture * future = nullptr;
        std::exception_ptr eptr;

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

        return future;
    }

    /// Start coroutine in background. It will take the ownership of `g` and
    /// free on finished.
    static void startInBackground(Ptr trx) { onFutureReady(nullptr, new Ptr(trx)); }

    AsyncTrxContext ctx;

private:
    std::vector<Step> steps;
    Callback on_finially;

    void abort(std::exception_ptr eptr) noexcept
    {
        if (!on_finially)
            return;

        try
        {
            on_finially(ctx, eptr);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__, "Uncaught exception in trx callback");
        }
    }

    /// Handle future produced by trx->next(). Invoke next() or abort() if finished.
    static void onFutureReady(FDBFuture * f, void * payload) noexcept
    {
        auto & trx = *static_cast<Ptr *>(payload);

#ifdef FDB_ASYNC_TRX_ENABLE_THREAD_POOL
        try
        {
            GlobalThreadPool::instance().scheduleOrThrow([trx, f]() {
#endif
                try
                {
                    if (auto * next_future = trx->next(f))
                        throwIfFDBError(fdb_future_set_callback(next_future, onFutureReady, new Ptr(trx)));
                    else
                        trx->abort(nullptr);
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
                        trx->abort(std::current_exception());
                    }
                }
                catch (...)
                {
                    trx->abort(std::current_exception());
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
            trx->ctx.deconstructVars();
            trx->ctx.gotoSet(0);
            onFutureReady(nullptr, new Ptr(trx));
        }
        catch (...)
        {
            trx->abort(std::current_exception());
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
        auto var_future = var<FDBFuturePtr>(nullptr, fdb_future_destroy);

        steps.emplace_back([var_future](Context & ctx, FDBFuture *) {
            auto & future = *ctx.getVar(var_future);

            future = FDBFuturePtr(fdb_transaction_get_versionstamp(ctx.getTrx()), fdb_future_destroy);
            return nullptr;
        });
        commit();
        steps.emplace_back([var_future](Context & ctx, FDBFuture *) { return ctx.getVar(var_future)->release(); });
        steps.emplace_back([var_versionstamp](Context & ctx, FDBFuture * f) {
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
        VarDesc<T> desc = {total_vars_size};
        total_vars_size += sizeof(T);

        // new T(std::forward<Args>(args)...);
        if constexpr (!std::is_trivial_v<T>)
            steps.emplace_back([desc, ... args = std::forward<Args>(args)](Context & ctx, FDBFuture *) mutable {
                new (ctx.getVar(desc)) T(std::forward<Args>(args)...);
                ctx.registerVar(desc);
                return nullptr;
            });

        return desc;
    }

    /// Alloc a variable in transaction context and set default variable.
    template <typename T, typename Default>
    std::enable_if_t<std::is_trivially_assignable_v<T &, Default>, VarDesc<T>> varDefault(Default && default_val)
    {
        VarDesc<T> desc{total_vars_size};
        total_vars_size += sizeof(T);

        steps.emplace_back([desc, default_val = std::forward<Default>(default_val)](Context & ctx, FDBFuture *) {
            (*ctx.getVar(desc)) = default_val;
            ctx.registerVar(desc);
            return nullptr;
        });

        return desc;
    }

    /// build() will take the ownership of the transaction.
    template <typename Callback>
    AsyncTrx::Ptr build(FDBTransaction * tr, Callback callback)
    {
        auto trx = std::make_shared<AsyncTrx>(std::move(steps), callback);
        trx->ctx = Context(tr, total_vars_size);

        return trx;
    }

    /// Set callback and exec the transaction.
    /// exec() will take the ownership of the transaction.
    template <typename Callback>
    void exec(FDBTransaction * tr, Callback callback)
    {
        AsyncTrx::startInBackground(build(tr, callback));
    }

private:
    std::vector<Step> steps;
    size_t total_vars_size = 0;
};

template <typename T>
using AsyncTrxVar = AsyncTrxContext::VarDesc<T>;
}
