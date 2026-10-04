#include <Processors/Executors/Runtime/Tools/Wyrd/Simulation/SimulatedThread.h>
#include <Common/CoroutineStack.h>

namespace DB::Runtime::Wyrd
{

SimulatedThread::SimulatedThread(UInt32 id_, size_t stack_size, std::function<void()> body_)
    : id(id_)
{
    auto routine = [this, body = std::move(body_)](auto & suspend_callback)
    {
        suspend_to_kernel = [&suspend_callback] { suspend_callback(); };
        body();
    };

    coroutine = StackfulCoroutine(CoroutineStack(stack_size), std::move(routine));
}

void SimulatedThread::resume()
{
    coroutine.resume();
}

void SimulatedThread::suspend()
{
    suspend_to_kernel();
}

bool SimulatedThread::finished() const
{
    return !coroutine;
}

}
