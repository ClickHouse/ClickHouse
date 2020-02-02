#include <unistd.h>
#include <syscall.h>
#include <common/getThreadId.h>


static thread_local uint64_t current_tid = 0;
uint64_t getThreadId()
{
    if (!current_tid)
        current_tid = syscall(SYS_gettid); /// This call is always successful. - man gettid

    return current_tid;
}
