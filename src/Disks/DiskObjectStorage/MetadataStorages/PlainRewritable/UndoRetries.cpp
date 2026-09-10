#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/UndoRetries.h>

#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>

#include <base/types.h>

#include <chrono>

namespace ProfileEvents
{
    extern const Event DiskPlainRewritableUndoStageRetries;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

/// A stage fails because object storage is unavailable, so the pause grows to the point where the retries cost nothing,
/// and stays short enough for a shutdown to be noticed at once.
constexpr UInt64 FIRST_PAUSE_MS = 100;
constexpr UInt64 MAX_PAUSE_MS = 1000;

}

void UndoRetries::runStage(const LoggerPtr & log, std::string_view description, const std::function<void()> & stage)
{
    UInt64 pause_ms = FIRST_PAUSE_MS;

    for (size_t attempt = 1;; ++attempt)
    {
        try
        {
            stage();
            return;
        }
        catch (...)
        {
            /// A logical error says that an invariant of this code does not hold, and repeating the stage cannot make
            /// it hold. It is also how a stage reports the one thing a reversal cannot repair: the blob is gone.
            if (getCurrentExceptionCode() == ErrorCodes::LOGICAL_ERROR)
                throw;

            ProfileEvents::increment(ProfileEvents::DiskPlainRewritableUndoStageRetries);
            tryLogCurrentException(log, fmt::format("Attempt {} to {} failed", attempt, description));

            std::unique_lock lock(mutex);
            if (shutdown_condition.wait_for(lock, std::chrono::milliseconds(pause_ms), [this] { return shutdown_called; }))
            {
                LOG_ERROR(
                    log,
                    "Stopped retrying to {} because the disk is shutting down. Object storage keeps a part of a "
                    "transaction that is reported as failed, and the next start loads the filesystem from object storage",
                    description);

                throw;
            }

            pause_ms = std::min(2 * pause_ms, MAX_PAUSE_MS);
        }
    }
}

void UndoRetries::shutdown()
{
    {
        std::lock_guard lock(mutex);
        shutdown_called = true;
    }

    shutdown_condition.notify_all();
}

}
