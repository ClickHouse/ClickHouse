#include <Processors/Executors/Runtime/Wyrd/Facade/Thread.h>

#if defined(WYRD_SIMULATION)
#    include <Processors/Executors/Runtime/Wyrd/Simulation/Syscall.h>
#    include <limits>
#elif defined(WYRD_ADVERSARY)
#    include <Processors/Executors/Runtime/Wyrd/Adversary/Access.h>
#endif

namespace DB::Runtime::Wyrd
{

#if defined(WYRD_SIMULATION)

Thread::Thread(std::function<void()> body)
    : done(std::make_shared<UInt32>(0))
{
    spawn(
        [body = std::move(body), flag = done]
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

Thread::Thread(std::function<void()> body)
    : impl(
          [body = std::move(body)]
          {
              if (auto * current = adversary())
                  current->threadEnter();
              body();
              if (auto * current = adversary())
                  current->threadExit();
          })
{
}

void Thread::join()
{
    impl.join();
}

#endif

}
