#pragma once
#include <sys/types.h>

namespace DB
{
/*
 * Waits for a specific pid with timeout
 * Returns `true` if process terminated successfully in specified timeout or `false` otherwise
 */
bool waitForPid(pid_t pid, size_t timeout_in_seconds);

#if defined(OS_LINUX)
int syscall_pidfd_open(pid_t pid);
int syscall_pidfd_send_signal(int pidfd, int sig);
#endif

}
