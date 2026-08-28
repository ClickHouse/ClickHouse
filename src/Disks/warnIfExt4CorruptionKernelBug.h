#pragma once

#include <base/types.h>

namespace DB
{

/// Adds a server warning when `directory` resides on ext4 - or its filesystem cannot be told -
/// under a Linux kernel affected by the 4.16.0-4.16.3 ext4 corruption bug (see #18794).
/// A directory that does not exist yet is probed through its nearest existing ancestor, the
/// filesystem it will be created on. No-op on other platforms and on unaffected kernels.
void warnIfAffectedByExt4CorruptionKernelBug(const String & directory, const String & description);

}
