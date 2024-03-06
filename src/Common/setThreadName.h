#pragma once
#include <string>

/** Sets the thread name (maximum length is 15 bytes),
  *  which will be visible in ps, gdb, /proc,
  *  for convenience of observation and debugging.
  */
void setThreadName(const char * name);

const char * getThreadName();
