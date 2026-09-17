#include <Disks/warnIfExt4CorruptionKernelBug.h>

#include <Common/VersionNumber.h>
#include <Common/filesystemHelpers.h>
#include <Interpreters/Context.h>
#include <Poco/Environment.h>

#include <algorithm>
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
    using RecordedMessage = Ext4CorruptionKernelBugWarningBatch::Recorded::Message;

    /// Every recorded message, not just the last: publishing drops those matching
    /// `warning_supress_regexp`, so a later suppressed probe must not erase an earlier one here.
    /// Guarded on its own so recording never touches the context's lock.
    std::mutex pending_warnings_mutex;
    std::vector<RecordedMessage> pending_warnings;
    /// Set by a flush that published an ext4 hit, cleared by one whose ext4 hits were all
    /// suppressed: only a warning that is really in place outranks later undetermined probes.
    std::atomic<bool> published_ext4{false};

    /// The batch the current thread stages into while one is open on it.
    thread_local Ext4CorruptionKernelBugWarningBatch::Recorded * staging = nullptr;

    bool holdsExt4Hit(const std::vector<RecordedMessage> & messages)
    {
        return std::ranges::any_of(messages, [](const auto & recorded) { return recorded.ext4; });
    }

    /// Only reached on Linux; the probe below is compiled out elsewhere.
    [[maybe_unused]] void recordWarning(PreformattedMessage message, bool ext4)
    {
        if (staging)
        {
            staging->messages.push_back({std::move(message), ext4});
            if (ext4)
                staging->ext4 = true;
            return;
        }
        std::lock_guard lock(pending_warnings_mutex);
        pending_warnings.push_back({std::move(message), ext4});
    }

    /// Whether an ext4 hit is kept by whatever the current probe records into: staged, queued for
    /// the next flush, or already published by one.
    [[maybe_unused]] bool ext4Recorded()
    {
        if ((staging && staging->ext4) || published_ext4)
            return true;
        std::lock_guard lock(pending_warnings_mutex);
        return holdsExt4Hit(pending_warnings);
    }
}

Ext4CorruptionKernelBugWarningBatch::Ext4CorruptionKernelBugWarningBatch()
    : outer(std::exchange(staging, &staged))
{
    /// An ext4 hit staged by the enclosing batch is kept exactly when this batch's commit is.
    if (outer)
        staged.ext4 = outer->ext4;
}

Ext4CorruptionKernelBugWarningBatch::~Ext4CorruptionKernelBugWarningBatch()
{
    staging = outer;
}

void Ext4CorruptionKernelBugWarningBatch::commit()
{
    auto begin = std::make_move_iterator(staged.messages.begin());
    auto end = std::make_move_iterator(staged.messages.end());
    if (outer)
    {
        outer->messages.insert(outer->messages.end(), begin, end);
        if (staged.ext4)
            outer->ext4 = true;
    }
    else
    {
        std::lock_guard lock(pending_warnings_mutex);
        pending_warnings.insert(pending_warnings.end(), begin, end);
    }
    staged.messages.clear();
}

size_t flushExt4CorruptionKernelBugWarning(const Context & context)
{
    std::vector<RecordedMessage> messages;
    {
        std::lock_guard lock(pending_warnings_mutex);
        messages.swap(pending_warnings);
    }

    /// Published in probe order, so the last unsuppressed one wins exactly as with direct publication.
    size_t published = 0;
    bool published_ext4_here = false;
    for (const auto & recorded : messages)
    {
        if (!context.addOrUpdateWarningMessage(Context::WarningType::LINUX_KERNEL_EXT4_CORRUPTION_BUG, recorded.message))
            continue;
        ++published;
        published_ext4_here |= recorded.ext4;
    }

    /// An ext4 hit only outranks later undetermined probes once it is published: one that
    /// `warning_supress_regexp` dropped leaves no warning in place to protect.
    if (holdsExt4Hit(messages))
        published_ext4 = published_ext4_here;
    return published;
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

        const String fs_type = getDirectoryFilesystemType(candidate.string());
        if (fs_type == "ext4")
        {
            recordWarning(PreformattedMessage::create(
                "This Linux kernel has a known ext4 filesystem corruption bug (fixed in 4.16.4) and {} ({}) resides on ext4. "
                "Consider upgrading the kernel.",
                description, directory), /* ext4 */ true);
        }
        else if (fs_type.empty() && !ext4Recorded())
        {
            /// A published or still kept ext4 hit outranks an undetermined probe, but a hit dropped
            /// with its batch or by suppression must not hide one: it is not a false alarm.
            recordWarning(PreformattedMessage::create(
                "This Linux kernel has a known ext4 filesystem corruption bug (fixed in 4.16.4) and the filesystem of {} ({}) "
                "could not be determined. Consider upgrading the kernel.",
                description, directory), /* ext4 */ false);
        }
    }
    catch (...) /// Ok: a failed probe must not break disk construction. // NOLINT(bugprone-empty-catch)
    {
    }
#endif
}

}
