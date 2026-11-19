#include <Common/Wyrd/Testing/Gate.h>
#include <Common/Wyrd/Facade/Futex.h>

namespace WYRD_NAMESPACE(DB::Wyrd)
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
