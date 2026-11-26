#include <Common/Wyrd/Facade/Thread.h>

#if defined(WYRD_ENABLED)
#    include <Common/Wyrd/Strategies/Simulation/Syscall.h>
#    include <Common/Wyrd/Strategies/Adversary/Syscall.h>
#    include <Common/Wyrd/Strategies/Mode.h>
#    include <exception>
#    include <limits>
#endif

namespace DB::Wyrd
{

#if defined(WYRD_ENABLED)

namespace SimulationMode
{

Thread::Thread(std::function<void()> body_, std::source_location location)
    : done(std::make_shared<UInt32>(0))
{
    spawn(
        [body = std::move(body_), flag = done]
        {
            body();
            *flag = 1;
            futexWake(flag.get(), std::numeric_limits<size_t>::max());
        },
        location);
}

void Thread::join(std::source_location location)
{
    while (*done == 0)
        futexWait(done.get(), location);
}

}

namespace AdversaryMode
{

Thread::Thread(std::function<void()> body_, std::source_location)
    : impl(
          [body = std::move(body_)]
          {
              enterThread();
              try
              {
                  body();
              }
              catch (...)
              {
                  failThread(std::current_exception());
              }
              exitThread();
          })
{
}

void Thread::join(std::source_location)
{
    impl.join();
}

}

WyrdThread::WyrdThread(std::function<void()> body_, std::source_location location)
{
    if (getMode() == Mode::Simulation)
        simulation_impl.emplace(std::move(body_), location);
    else
        adversary_impl.emplace(std::move(body_), location);
}

void WyrdThread::join(std::source_location location)
{
    if (getMode() == Mode::Simulation)
        simulation_impl->join(location);
    else
        adversary_impl->join(location);
}

#endif

}
