#pragma once

#include <cstddef>

/// IO helpers

/// Write loop with EINTR handling.
///
/// This function is safe to use in static initializers.
///
/// @param size - len of @data or 0 to use strlen()
/// @return true if write was succeed, otherwise false.
bool writeRetry(int fd, const char * data, size_t size = 0);
