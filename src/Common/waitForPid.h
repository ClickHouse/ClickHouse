#pragma once
#include <sys/types.h>

namespace DB
{
/*
 * Waits for a specific pid with timeout, using modern Linux and OSX facilities
 * Returns `true` if process terminated successfully or `false` otherwise
 */
bool waitForPid(pid_t pid, size_t timeout_in_seconds);

}
