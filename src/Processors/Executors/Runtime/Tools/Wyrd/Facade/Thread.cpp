#include <Processors/Executors/Runtime/Tools/Wyrd/Facade/Thread.h>

#if defined(EXECUTOR_SIM)
#    include <Processors/Executors/Runtime/Tools/Wyrd/Simulation/Syscall.h>
#    include <limits>
#elif defined(EXECUTOR_FAULT_INJECTION)
#    include <Processors/Executors/Runtime/Tools/Wyrd/Adversary/Access.h>
#endif

namespace DB::Runtime::Wyrd
{

#if defined(EXECUTOR_SIM)

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

#elif defined(EXECUTOR_FAULT_INJECTION)

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
