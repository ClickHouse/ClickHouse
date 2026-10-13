#include <Processors/Executors/Runtime/Wyrd/Strategies/Simulation/SimulatedThread.h>

#include <boost/context/protected_fixedsize_stack.hpp>

#include <memory>

namespace DB::Runtime::Wyrd
{

SimulatedThread::SimulatedThread(UInt32 id_, size_t stack_size, std::function<void()> body_)
    : id(id_)
{
    auto routine = [this, body = std::move(body_)](boost::context::fiber && caller)
    {
        kernel = std::move(caller);
        body();
        return std::move(kernel);
    };

    fiber = boost::context::fiber(std::allocator_arg, boost::context::protected_fixedsize_stack(stack_size), std::move(routine));
}

void SimulatedThread::resume()
{
    fiber = std::move(fiber).resume();
}

void SimulatedThread::suspend()
{
    kernel = std::move(kernel).resume();
}

bool SimulatedThread::finished() const
{
    return !fiber;
}

}
