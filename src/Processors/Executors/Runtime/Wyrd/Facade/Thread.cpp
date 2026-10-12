#include <Processors/Executors/Runtime/Wyrd/Facade/Thread.h>

#if defined(WYRD_SIMULATION)
#    include <Processors/Executors/Runtime/Wyrd/Strategies/Simulation/Syscall.h>
#    include <limits>
#elif defined(WYRD_ADVERSARY)
#    include <Processors/Executors/Runtime/Wyrd/Strategies/Adversary/Syscall.h>
#endif

namespace DB::Runtime::Wyrd
{

#if defined(WYRD_SIMULATION)

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

#elif defined(WYRD_ADVERSARY)

Thread::Thread(std::function<void()> body_, std::source_location)
    : impl(
          [body = std::move(body_)]
          {
              enterThread();
              body();
              exitThread();
          })
{
}

void Thread::join(std::source_location)
{
    impl.join();
}

#endif

}
