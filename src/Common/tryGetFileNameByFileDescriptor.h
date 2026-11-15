#pragma once

#include <optional>
#include <base/types.h>

namespace DB
{
/// Supports only Linux/MacOS. On other platforms, returns nullopt.
std::optional<String> tryGetFileNameFromFileDescriptor(int fd);
}
