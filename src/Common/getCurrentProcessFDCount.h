#pragma once

#include <base/types.h>

/// Get current process file descriptor count
/// @return -1 os doesn't support "lsof" command or some error occurs.
Int64 getCurrentProcessFDCount();
