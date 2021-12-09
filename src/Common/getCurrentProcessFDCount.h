#pragma once

/// Get current process file descriptor count
/// @return -1 os doesn't support "lsof" command or some error occurs.
int getCurrentProcessFDCount();
