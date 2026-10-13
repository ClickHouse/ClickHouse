#pragma once

#include <base/types.h>

#include <boost/context/fiber.hpp>

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
    boost::context::fiber fiber;
    boost::context::fiber kernel;
};

}
