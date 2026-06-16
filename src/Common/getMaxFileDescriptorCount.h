#pragma once

#include <optional>

/// Get process max file descriptor count
/// @return std::nullopt if os does not support getrlimit command or some error occurs
std::optional<size_t> getMaxFileDescriptorCount();

