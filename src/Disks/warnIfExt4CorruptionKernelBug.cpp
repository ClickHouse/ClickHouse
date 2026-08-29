#include <Disks/warnIfExt4CorruptionKernelBug.h>

#include <Common/VersionNumber.h>
#include <Common/filesystemHelpers.h>
#include <Interpreters/Context.h>
#include <Poco/Environment.h>

#include <atomic>
#include <filesystem>
#include <mutex>
#include <optional>

namespace fs = std::filesystem;

namespace DB
{

namespace
{
    /// One slot, since every probe reports under the same warning type and the last write wins
    /// there anyway. Guarded on its own so recording never touches the context's lock.
    std::mutex pending_warning_mutex;
    std::optional<PreformattedMessage> pending_warning;

    void recordWarning(PreformattedMessage message)
    {
        std::lock_guard lock(pending_warning_mutex);
        pending_warning = std::move(message);
    }
}

void flushExt4CorruptionKernelBugWarning(const Context & context)
{
    std::optional<PreformattedMessage> message;
    {
        std::lock_guard lock(pending_warning_mutex);
        message.swap(pending_warning);
    }
    /// Taken rather than copied: publishing stores it, so re-publishing on every read would be
    /// wasted work. A probe that fires later simply refills the slot.
    if (message)
        context.addOrUpdateWarningMessage(Context::WarningType::LINUX_KERNEL_EXT4_CORRUPTION_BUG, *message);
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
