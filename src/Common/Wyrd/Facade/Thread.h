#pragma once


#include <functional>
#include <source_location>

#if defined(WYRD_ENABLED)
#    include <Common/ThreadPool.h>
#    include <base/types.h>
#    include <memory>
#    include <optional>
#else
#    include <Common/ThreadPool.h>
#endif

namespace DB::Wyrd
{

#if defined(WYRD_ENABLED)

namespace SimulationMode
{

class Thread
{
public:
    explicit Thread(std::function<void()> body_, std::source_location location = std::source_location::current());

    void join(std::source_location location = std::source_location::current());

private:
    std::shared_ptr<UInt32> done;
};

}

namespace AdversaryMode
{

class Thread
{
public:
    explicit Thread(std::function<void()> body_, std::source_location location = std::source_location::current());

    void join(std::source_location location = std::source_location::current());

private:
    ThreadFromGlobalPool impl;
};

}

class WyrdThread
{
public:
    explicit WyrdThread(std::function<void()> body_, std::source_location location = std::source_location::current());

    void join(std::source_location location = std::source_location::current());

private:
    std::optional<SimulationMode::Thread> simulation_impl;
    std::optional<AdversaryMode::Thread> adversary_impl;
};

using Thread = WyrdThread;

#else

using Thread = ThreadFromGlobalPool;

#endif

}
