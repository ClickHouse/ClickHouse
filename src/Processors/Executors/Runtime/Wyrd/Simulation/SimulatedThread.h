#pragma once

#include <Common/StackfulCoroutine.h>
#include <base/types.h>

#include <functional>

namespace DB::Runtime::Wyrd
{

struct SimulatedThread
{
    SimulatedThread(UInt32 id_, size_t stack_size, std::function<void()> body_);

    void resume();
    void suspend();
    bool finished() const;

    const UInt32 id;

private:
    std::function<void()> suspend_to_kernel;
    StackfulCoroutine coroutine;
};

}
