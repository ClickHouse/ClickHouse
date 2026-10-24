#pragma once

#include "config.h"

#include <mutex>

#if USE_SILK

#include <Common/SilkFiberScheduler.h>
#include <base/defines.h>
#include <silk/fibers/mutex.h>

#include <variant>

namespace Silk
{

class TSA_CAPABILITY("FiberMutex") FiberMutex
{
public:
    FiberMutex()
        : impl(isFiberSchedulerInitialized() ? Impl(std::in_place_type<silk::FiberMutex>) : Impl(std::in_place_type<std::mutex>))
    {
    }

    void lock() TSA_ACQUIRE()
    {
        std::visit([](auto & mutex) TSA_NO_THREAD_SAFETY_ANALYSIS { mutex.lock(); }, impl);
    }

    bool try_lock() TSA_TRY_ACQUIRE(true)
    {
        return std::visit([](auto & mutex) TSA_NO_THREAD_SAFETY_ANALYSIS { return mutex.try_lock(); }, impl);
    }

    void unlock() TSA_RELEASE()
    {
        std::visit([](auto & mutex) TSA_NO_THREAD_SAFETY_ANALYSIS { mutex.unlock(); }, impl);
    }

private:
    using Impl = std::variant<std::mutex, silk::FiberMutex>;

    Impl impl;
};

}

#else

namespace Silk
{

using FiberMutex = std::mutex;

}

#endif
