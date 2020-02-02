#include <unistd.h>
#include <syscall.h>


static thread_local unsigned current_tid = 0;
unsigned getThreadId()
{
    if (!current_tid)
        current_tid = syscall(SYS_gettid); /// This call is always successful. - man gettid

    return current_tid;
}
