#pragma once

/// Get process max file descriptor count
/// @return -1 if os does not support ulimit command or some error occurs
int getMaxFileDescriptorCount();

