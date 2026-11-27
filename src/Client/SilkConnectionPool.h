#pragma once

#include "config.h"

#if USE_SILK

#include <Client/ConnectionPool.h>

#include <silk/fibers/condvar.h>
#include <silk/fibers/mutex.h>

#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdint>

namespace Silk
{

class FiberCondVarAdapter
{
public:
    template <typename Lockable>
    void wait(Lockable & lock) noexcept
    {
        cond_var.wait(lock);
    }

    template <typename Lockable, typename Rep, typename Period>
    std::cv_status wait_for(Lockable & lock, const std::chrono::duration<Rep, Period> & timeout) noexcept /// NOLINT(readability-identifier-naming)
    {
        auto nanoseconds = std::chrono::duration_cast<std::chrono::nanoseconds>(timeout);
        int result = cond_var.wait_for(lock, static_cast<uint64_t>(nanoseconds.count()));
        return result == ETIMEDOUT ? std::cv_status::timeout : std::cv_status::no_timeout;
    }

    void notify_one() noexcept /// NOLINT(readability-identifier-naming)
    {
        cond_var.notify_one();
    }

    void notify_all() noexcept /// NOLINT(readability-identifier-naming)
    {
        cond_var.notify_all();
    }

private:
    silk::FiberCondVar cond_var;
};

using ConnectionPool = DB::ConnectionPoolImpl<silk::FiberMutex, FiberCondVarAdapter>;

}

#endif
