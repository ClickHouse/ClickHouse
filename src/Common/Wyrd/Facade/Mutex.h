#pragma once

#include <base/defines.h>

#include <mutex>
#include <source_location>
#include <string>
#include <utility>

#include <fmt/format.h>

#if defined(WYRD_ENABLED)
#    include <Common/Wyrd/Facade/Atomic.h>
#    include <Common/Wyrd/Strategies/Mode.h>
#    include <base/types.h>
#    include <optional>
#endif

namespace DB::Wyrd
{

#if defined(WYRD_ENABLED)

namespace SimulationMode
{

class TSA_CAPABILITY("mutex") Mutex
{
public:
    void lock(std::source_location location = std::source_location::current()) TSA_ACQUIRE();
    bool try_lock(std::source_location location = std::source_location::current()) TSA_TRY_ACQUIRE(true);
    void unlock(std::source_location location = std::source_location::current()) TSA_RELEASE();

    void setName(std::string name);

private:
    Atomic<UInt32> locked{0};
};

template <typename... Args>
void setName(Mutex & mutex, fmt::format_string<Args...> format, Args &&... args)
{
    mutex.setName(fmt::format(format, std::forward<Args>(args)...));
}

}

namespace AdversaryMode
{

class TSA_CAPABILITY("mutex") Mutex
{
public:
    void lock(std::source_location location = std::source_location::current()) TSA_ACQUIRE();
    bool try_lock(std::source_location location = std::source_location::current()) TSA_TRY_ACQUIRE(true);
    void unlock(std::source_location location = std::source_location::current()) TSA_RELEASE();

private:
    Atomic<UInt32> locked{0};
};

template <typename... Args>
void setName(Mutex &, fmt::format_string<Args...>, Args &&...)
{
}

}

class TSA_CAPABILITY("mutex") WyrdMutex
{
public:
    WyrdMutex();

    void lock(std::source_location location = std::source_location::current()) TSA_ACQUIRE() TSA_NO_THREAD_SAFETY_ANALYSIS;
    bool try_lock(std::source_location location = std::source_location::current()) TSA_TRY_ACQUIRE(true) TSA_NO_THREAD_SAFETY_ANALYSIS;
    void unlock(std::source_location location = std::source_location::current()) TSA_RELEASE() TSA_NO_THREAD_SAFETY_ANALYSIS;

    SimulationMode::Mutex & simulation();
    AdversaryMode::Mutex & adversary();

private:
    std::optional<SimulationMode::Mutex> simulation_impl;
    std::optional<AdversaryMode::Mutex> adversary_impl;
};

using Mutex = WyrdMutex;

template <typename... Args>
void setName(Mutex & mutex, fmt::format_string<Args...> format, Args &&... args)
{
    if (getMode() == Mode::Simulation)
        SimulationMode::setName(mutex.simulation(), format, std::forward<Args>(args)...);
    else
        AdversaryMode::setName(mutex.adversary(), format, std::forward<Args>(args)...);
}

#else

using Mutex = std::mutex;

template <typename... Args>
void setName(Mutex &, fmt::format_string<Args...>, Args &&...)
{
}

#endif

}
