#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/UndoWithRetries.h>

#include <Common/DynamicDelay.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>

#include <base/sleep.h>
#include <base/types.h>

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

/// A stage fails because object storage is unavailable, so the pause grows until the retries cost nothing.
constexpr double FIRST_PAUSE_MS = 100;
constexpr double MAX_PAUSE_MS = 5000;
constexpr double PAUSE_FACTOR = 2;
/// Every transaction that object storage is failing right now retries on the same schedule, so each pause is spread to
/// keep them from coming back at the same moment.
constexpr Int64 MAX_JITTER_MS = 100;

}

void undoWithRetries(const LoggerPtr & log, std::string_view description, const std::function<void()> & stage)
{
    DynamicDelay pause;
    pause.setConfiguration(FIRST_PAUSE_MS, MAX_PAUSE_MS, PAUSE_FACTOR);

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

            sleepForMilliseconds(pause.getCurrentDelayWithJitter(0, MAX_JITTER_MS));
            pause.up();
        }
    }
}

}
