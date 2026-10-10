#include <Processors/Executors/Runtime/Wyrd/Testing/Gate.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Futex.h>

namespace DB::Runtime::Wyrd
{

void Gate::pass()
{
    UInt32 seen = is_open.load();
    while (seen == 0)
        futexWaitFetch(is_open, seen);
}

void Gate::open()
{
    is_open.store(1);
    futexWakeAll(is_open);
}

void Gate::close()
{
    is_open.store(0);
}

}
