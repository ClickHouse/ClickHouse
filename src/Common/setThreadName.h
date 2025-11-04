#pragma once
#include <string>

constexpr size_t MAX_THREAD_NAME_SIZE = 15;

/** Sets the thread name (maximum length is 15 bytes),
  *  which will be visible in ps, gdb, /proc,
  *  for convenience of observation and debugging.
  *
  * @param truncate - if true, will truncate to 15 automatically, otherwise throw
  */
void setThreadName(const char * name, bool truncate = false);

const char * getThreadName();
