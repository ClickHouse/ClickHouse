#pragma once

#include <base/types.h>

namespace DB
{

class Context;

/// Records a server warning when `directory` resides on ext4 - or its filesystem cannot be told -
/// under a Linux kernel affected by the 4.16.0-4.16.3 ext4 corruption bug (see #18794).
/// A directory that does not exist yet is probed through its nearest existing ancestor, the
/// filesystem it will be created on. No-op on other platforms and on unaffected kernels.
///
/// The finding is only recorded here, not published: disks are constructed from call sites that
/// already hold `Context::shared->mutex`, which is not recursive, and publishing takes it. A
/// constructor cannot prove its caller's lock state, so publication is deferred to the drain below.
void warnIfAffectedByExt4CorruptionKernelBug(const String & directory, const String & description);

/// Publishes whatever the probes above recorded, logging it and storing it for `system.warnings`.
/// Must be called without `Context::shared->mutex`. The server calls it once startup is complete,
/// so this is a real startup warning; `Context::getWarnings` calls it too, to catch disks built
/// later.
void flushExt4CorruptionKernelBugWarning(const Context & context);

}
