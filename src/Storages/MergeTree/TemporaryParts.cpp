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

    /// Wait out a cleanup of the same name (bounded by one directory removal), so the claim cannot
    /// recreate the directory under the cleaner's feet.
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

scope_guard TemporaryParts::tryHoldForCleanup(const std::string & basename)
{
    {
        std::lock_guard lock(mutex);
        if (parts.contains(basename))
            return {};
        if (!being_cleaned.emplace(basename).second)
            return {};
    }

    /// Runs from the guard's destructor, so it must not throw.
    return [this, basename]
    {
        {
            std::lock_guard lock(mutex);
            being_cleaned.erase(basename);
        }
        cleanup_finished.notify_all();
    };
}

}
