#pragma once


#include <functional>
#include <source_location>

#if defined(WYRD_SIMULATION)
#    include <base/types.h>
#    include <memory>
#else
#    include <Common/ThreadPool.h>
#endif

namespace DB::Runtime::Wyrd
{

#if defined(WYRD_SIMULATION)

class Thread
{
public:
    explicit Thread(std::function<void()> body_, std::source_location location = std::source_location::current());

    void join(std::source_location location = std::source_location::current());

private:
    std::shared_ptr<UInt32> done;
};

#elif defined(WYRD_ADVERSARY)

class Thread
{
public:
    explicit Thread(std::function<void()> body_, std::source_location location = std::source_location::current());

    void join(std::source_location location = std::source_location::current());

private:
    ThreadFromGlobalPool impl;
};

#else

using Thread = ThreadFromGlobalPool;

#endif

}
