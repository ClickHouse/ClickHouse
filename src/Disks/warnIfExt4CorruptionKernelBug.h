#pragma once

#include <base/types.h>
#include <Common/LoggingFormatStringHelpers.h>

#include <boost/noncopyable.hpp>
#include <vector>

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

/// Stages what the probes on the current thread record until `commit()`. A batch destroyed
/// uncommitted, i.e. by the exception that aborted a disk selector update, discards them: those
/// disks never became active, so nothing is published for them.
class Ext4CorruptionKernelBugWarningBatch : private boost::noncopyable
{
public:
    Ext4CorruptionKernelBugWarningBatch();
    ~Ext4CorruptionKernelBugWarningBatch();
    void commit();

private:
    std::vector<PreformattedMessage> staged;
    std::vector<PreformattedMessage> * outer;
};

/// Publishes whatever the probes above recorded, logging it and storing it for `system.warnings`,
/// and returns how many messages it published. Must be called without `Context::shared->mutex`.
/// The server calls it once startup is complete and `Context::updateStorageConfiguration` after
/// every reload, so both are real server warnings; `Context::getWarnings` calls it too, to catch
/// disks built on any other path.
size_t flushExt4CorruptionKernelBugWarning(const Context & context);

}
