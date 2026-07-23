#include <Storages/MergeTree/TemporaryParts.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

bool TemporaryParts::contains(const std::string & basename) const
{
    std::lock_guard lock(mutex);
    return parts.contains(basename);
}

void TemporaryParts::add(const std::string & basename)
{
    std::unique_lock lock(mutex);

    /// If the background cleaner is deleting a leftover directory with this name, wait for it to
    /// finish: the hold is transient (bounded by one directory removal), so a legitimate claim waits
    /// it out instead of racing it and recreating the directory under the cleaner's feet.
    cleanup_finished.wait(lock, [&] { return !being_cleaned.contains(basename); });

    bool inserted = parts.emplace(basename).second;
    if (!inserted)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Temporary part {} already added", basename);
}

void TemporaryParts::remove(const std::string & basename)
{
    std::lock_guard lock(mutex);
    bool removed = parts.erase(basename);
    if (!removed)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Temporary part {} does not exist", basename);
}

bool TemporaryParts::tryClaimForCleanup(const std::string & basename)
{
    std::lock_guard lock(mutex);
    if (parts.contains(basename))
        return false;
    return being_cleaned.emplace(basename).second;
}

void TemporaryParts::releaseCleanupClaim(const std::string & basename)
{
    {
        std::lock_guard lock(mutex);
        bool removed = being_cleaned.erase(basename);
        if (!removed)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Temporary part {} is not being cleaned", basename);
    }
    cleanup_finished.notify_all();
}

}
