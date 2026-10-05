#include <Processors/Executors/Runtime/Wyrd/Facade/Thread.h>

#if defined(WYRD_SIMULATION)
#    include <Processors/Executors/Runtime/Wyrd/Simulation/Syscall.h>
#    include <limits>
#elif defined(WYRD_ADVERSARY)
#    include <Processors/Executors/Runtime/Wyrd/Adversary/Syscall.h>
#endif

namespace DB::Runtime::Wyrd
{

#if defined(WYRD_SIMULATION)

Thread::Thread(std::function<void()> body_)
    : done(std::make_shared<UInt32>(0))
{
    spawn(
        [body = std::move(body_), flag = done]
        {
            body();
            *flag = 1;
            futexWake(flag.get(), std::numeric_limits<size_t>::max());
        });
}

void Thread::join()
{
    while (*done == 0)
        futexWait(done.get());
}

#elif defined(WYRD_ADVERSARY)

Thread::Thread(std::function<void()> body_)
    : impl(
          [body = std::move(body_)]
          {
              enterThread();
              body();
              exitThread();
          })
{
}

void Thread::join()
{
    impl.join();
}

#endif

}
