#include <Disks/warnIfExt4CorruptionKernelBug.h>

#include <Common/VersionNumber.h>
#include <Common/filesystemHelpers.h>
#include <Interpreters/Context.h>
#include <Poco/Environment.h>

#include <atomic>
#include <filesystem>
#include <iterator>
#include <mutex>
#include <utility>
#include <vector>

namespace fs = std::filesystem;

namespace DB
{

namespace
{
    /// Every recorded message, not just the last: publishing drops those matching
    /// `warning_supress_regexp`, so a later suppressed probe must not erase an earlier one here.
    /// Guarded on its own so recording never touches the context's lock.
    std::mutex pending_warnings_mutex;
    std::vector<PreformattedMessage> pending_warnings;

    /// The batch the current thread stages into while one is open on it.
    thread_local std::vector<PreformattedMessage> * staging = nullptr;

    /// Only reached on Linux; the probe below is compiled out elsewhere.
    [[maybe_unused]] void recordWarning(PreformattedMessage message)
    {
        if (staging)
        {
            staging->push_back(std::move(message));
            return;
        }
        std::lock_guard lock(pending_warnings_mutex);
        pending_warnings.push_back(std::move(message));
    }
}

Ext4CorruptionKernelBugWarningBatch::Ext4CorruptionKernelBugWarningBatch()
    : outer(std::exchange(staging, &staged))
{
}

Ext4CorruptionKernelBugWarningBatch::~Ext4CorruptionKernelBugWarningBatch()
{
    staging = outer;
}

void Ext4CorruptionKernelBugWarningBatch::commit()
{
    auto begin = std::make_move_iterator(staged.begin());
    auto end = std::make_move_iterator(staged.end());
    if (outer)
    {
        outer->insert(outer->end(), begin, end);
    }
    else
    {
        std::lock_guard lock(pending_warnings_mutex);
        pending_warnings.insert(pending_warnings.end(), begin, end);
    }
    staged.clear();
}

size_t flushExt4CorruptionKernelBugWarning(const Context & context)
{
    std::vector<PreformattedMessage> messages;
    {
        std::lock_guard lock(pending_warnings_mutex);
        messages.swap(pending_warnings);
    }
    /// Published in probe order, so the last unsuppressed one wins exactly as with direct publication.
    for (const auto & message : messages)
        context.addOrUpdateWarningMessage(Context::WarningType::LINUX_KERNEL_EXT4_CORRUPTION_BUG, message);
    return messages.size();
}

void warnIfAffectedByExt4CorruptionKernelBug([[maybe_unused]] const String & directory, [[maybe_unused]] const String & description)
{
#if defined(OS_LINUX)
    static const bool affected_kernel = []
    {
        VersionNumber linux_version(Poco::Environment::osVersion());
        return linux_version >= VersionNumber{4, 16, 0} && linux_version < VersionNumber{4, 16, 4};
    }();
    if (!affected_kernel)
        return;

    try
    {
        fs::path candidate(directory);
        std::error_code ec;
        while (!candidate.empty() && candidate != candidate.parent_path() && !fs::is_directory(candidate, ec))
            candidate = candidate.parent_path();
        /// A relative root walks up to an empty path rather than to "/", and the filesystem it will
        /// be created on is the working directory's.
        if (candidate.empty())
            candidate = fs::current_path(ec);
        if (candidate.empty() || !fs::is_directory(candidate, ec))
            return;

        /// A determined ext4 hit must not be downgraded by a later undetermined probe.
        static std::atomic<bool> reported_ext4{false};
        const String fs_type = getDirectoryFilesystemType(candidate.string());
        if (fs_type == "ext4")
        {
            reported_ext4 = true;
            recordWarning(PreformattedMessage::create(
                "This Linux kernel has a known ext4 filesystem corruption bug (fixed in 4.16.4) and {} ({}) resides on ext4. "
                "Consider upgrading the kernel.",
                description, directory));
        }
        else if (fs_type.empty() && !reported_ext4)
        {
            /// An unreadable /proc/self/mounts must not trade the false alarm for a blind spot.
            recordWarning(PreformattedMessage::create(
                "This Linux kernel has a known ext4 filesystem corruption bug (fixed in 4.16.4) and the filesystem of {} ({}) "
                "could not be determined. Consider upgrading the kernel.",
                description, directory));
        }
    }
    catch (...) /// Ok: a failed probe must not break disk construction. // NOLINT(bugprone-empty-catch)
    {
    }
#endif
}

}
