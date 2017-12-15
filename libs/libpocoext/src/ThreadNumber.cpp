#include <pthread.h>
#include <common/likely.h>

#include <Poco/Ext/ThreadNumber.h>


static thread_local unsigned thread_number = 0;
static unsigned threads = 0;

unsigned Poco::ThreadNumber::get()
{
    if (unlikely(thread_number == 0))
        thread_number = __sync_add_and_fetch(&threads, 1);

    return thread_number;
}
