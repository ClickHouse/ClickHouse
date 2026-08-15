#include <Common/Exception.h>
#include <Common/FailPoint.h>

#include <boost/core/noncopyable.hpp>
#include <condition_variable>
#include <mutex>


namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int SUPPORT_IS_DISABLED;
};

#if USE_LIBFIU
static struct InitFiu
{
    InitFiu()
    {
        fiu_init(0);
    }
} init_fiu;
#endif

/// The list of failpoints lives in its own file so that `.gitattributes` can mark it `merge=union`.
#include <Common/FailPointsList.inc>

namespace FailPoints
{
#define M(NAME) extern const char(NAME)[] = #NAME "";
APPLY_FOR_FAILPOINTS(M, M, M, M)
#undef M
}

#if USE_LIBFIU

std::unordered_map<String, std::shared_ptr<FailPointChannel>> FailPointInjection::fail_point_wait_channels;
std::mutex FailPointInjection::mu;

struct FailPointChannel
{
    /// Condition variable for target threads to wait when paused at failpoint
    std::condition_variable pause_cv;

    /// Condition variable for target threads to wait for resume notification
    std::condition_variable resume_cv;

    /// Resume epoch: incremented on each notify or disable to wake up waiting threads.
    /// Threads record the epoch when they start waiting, and only wake up
    /// if the current epoch is greater than their recorded epoch.
    size_t resume_epoch = 0;

    /// Pause epoch: incremented each time a thread pauses at this failpoint.
    /// Used by waitForPause to distinguish new pauses from stale ones:
    /// after a notify, waitForPause waits for pause_epoch > resume_epoch,
    /// ensuring the pause happened after the most recent resume.
    size_t pause_epoch = 0;

    /// Set to true by disableFailPoint so that waitForPause can return
    /// even when no thread has paused (pause_epoch <= resume_epoch).
    bool disabled = false;
};

void FailPointInjection::pauseFailPoint(const String & fail_point_name)
{
    fiu_do_on(fail_point_name.c_str(), FailPointInjection::notifyPauseAndWaitForResume(fail_point_name););
}

bool FailPointInjection::hasAnyFailPointBeenRegistered()
{
    return atomic_load_explicit(&has_any_failpoint_been_registered, memory_order_relaxed) != 0;
}

void FailPointInjection::enableFailPoint(const String & fail_point_name)
{
#define SUB_M(NAME, flags, pause)                                                                               \
    if (fail_point_name == FailPoints::NAME)                                                                    \
    {                                                                                                           \
        /* FIU_ONETIME -- Only fail once; the point of failure will be automatically disabled afterwards.*/     \
        fiu_enable(FailPoints::NAME, 1, nullptr, flags);                                                        \
        if (pause)                                                                                               \
        {                                                                                                       \
            std::lock_guard lock(mu);                                                                           \
            fail_point_wait_channels.try_emplace(FailPoints::NAME, std::make_shared<FailPointChannel>());       \
        }                                                                                                       \
        return;                                                                                                 \
    }
#define ONCE(NAME) SUB_M(NAME, FIU_ONETIME, 0)
#define REGULAR(NAME) SUB_M(NAME, 0, 0)
#define PAUSEABLE_ONCE(NAME) SUB_M(NAME, FIU_ONETIME, 1)
#define PAUSEABLE(NAME) SUB_M(NAME, 0, 1)
    APPLY_FOR_FAILPOINTS(ONCE, REGULAR, PAUSEABLE_ONCE, PAUSEABLE)
#undef SUB_M
#undef ONCE
#undef REGULAR
#undef PAUSEABLE_ONCE
#undef PAUSEABLE

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot find fail point {}", fail_point_name);
}

static bool isRegisteredFailPoint(const String & fail_point_name)
{
#define M(NAME)                              \
    if (fail_point_name == FailPoints::NAME) \
        return true;
    APPLY_FOR_FAILPOINTS(M, M, M, M)
#undef M

    return false;
}

void FailPointInjection::disableFailPoint(const String & fail_point_name)
{
    /// Registration is deliberately the only check: disabling a registered fail point that is
    /// not currently enabled must stay a silent no-op, because callers do idempotent cleanup.
    if (!isRegisteredFailPoint(fail_point_name))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot find fail point {}", fail_point_name);

    std::lock_guard lock(mu);
    if (auto iter = fail_point_wait_channels.find(fail_point_name); iter != fail_point_wait_channels.end())
    {
        /// Increment resume_epoch to wake up all waiting threads.
        ++iter->second->resume_epoch;
        iter->second->disabled = true;
        iter->second->resume_cv.notify_all();
        iter->second->pause_cv.notify_all();
        fail_point_wait_channels.erase(iter);
    }
    fiu_disable(fail_point_name.c_str());
}

void FailPointInjection::notifyFailPoint(const String & fail_point_name)
{
    /// Reported separately from the missing channel below, so a typo is not described as a
    /// registered fail point that nothing is waiting on.
    if (!isRegisteredFailPoint(fail_point_name))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot find fail point {}", fail_point_name);

    std::lock_guard lock(mu);
    if (auto iter = fail_point_wait_channels.find(fail_point_name); iter != fail_point_wait_channels.end())
    {
        /// Increment resume_epoch to mark a new notification cycle
        ++iter->second->resume_epoch;
        iter->second->resume_cv.notify_all();
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot find channel for fail point {}", fail_point_name);
    }
}

void FailPointInjection::notifyPauseAndWaitForResume(const String & fail_point_name)
{
    std::unique_lock lock(mu);
    auto iter = fail_point_wait_channels.find(fail_point_name);
    if (iter == fail_point_wait_channels.end())
        return;

    auto channel = iter->second;
    size_t my_resume_epoch = channel->resume_epoch;

    /// Signal that a thread has reached and paused at this failpoint
    ++channel->pause_epoch;
    channel->pause_cv.notify_all();

    /// Wait for resume_epoch to be incremented by notify or disable
    channel->resume_cv.wait(lock, [&] {
        return channel->resume_epoch > my_resume_epoch;
    });
}

void FailPointInjection::waitForPause(const String & fail_point_name)
{
    /// A mistyped name would otherwise return at once and silently drop the synchronisation the
    /// caller asked for, turning a deterministic test into a race. A registered fail point with no
    /// channel still returns, because it is simply not paused.
    if (!isRegisteredFailPoint(fail_point_name))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot find fail point {}", fail_point_name);

    std::unique_lock lock(mu);
    auto iter = fail_point_wait_channels.find(fail_point_name);
    if (iter == fail_point_wait_channels.end())
        return;

    auto channel = iter->second;

    /// Wait until a thread has paused at this failpoint after the most recent resume.
    channel->pause_cv.wait(lock, [&] {
        return channel->pause_epoch > channel->resume_epoch || channel->disabled;
    });
}

void FailPointInjection::waitForResume(const String & fail_point_name)
{
    /// Same as `waitForPause`: an unknown name must not look like an already finished wait.
    if (!isRegisteredFailPoint(fail_point_name))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot find fail point {}", fail_point_name);

    std::unique_lock lock(mu);
    auto iter = fail_point_wait_channels.find(fail_point_name);
    if (iter == fail_point_wait_channels.end())
        return;

    auto channel = iter->second;
    size_t my_resume_epoch = channel->resume_epoch;

    /// Wait for resume_epoch to be incremented by notify or disable
    channel->resume_cv.wait(lock, [&] {
        return channel->resume_epoch > my_resume_epoch;
    });
}

std::vector<FailPointInjection::FailPointInfo> FailPointInjection::getFailPoints()
{
    std::vector<FailPointInfo> result;

#define SUB_M(NAME, TP)                                   \
    result.push_back(                                     \
        FailPointInfo{                                    \
            .name = FailPoints::NAME,                     \
            .type = FailPointType::TP,                    \
            .enabled = fiu_status(FailPoints::NAME) != 0, \
        });
#define ADD_ONCE(NAME) SUB_M(NAME, Once)
#define ADD_REGULAR(NAME) SUB_M(NAME, Regular)
#define ADD_PAUSEABLE_ONCE(NAME) SUB_M(NAME, PauseableOnce)
#define ADD_PAUSEABLE(NAME) SUB_M(NAME, Pauseable)
    APPLY_FOR_FAILPOINTS(ADD_ONCE, ADD_REGULAR, ADD_PAUSEABLE_ONCE, ADD_PAUSEABLE)
#undef SUB_M
#undef ADD_ONCE
#undef ADD_REGULAR
#undef ADD_PAUSEABLE_ONCE
#undef ADD_PAUSEABLE

    return result;
}

#else // USE_LIBFIU

/// These are hooks in regular code paths, so they must be no-ops rather than throw.
/// In particular, `disableFailPoint` is called unconditionally during quorum cleanup
/// in `StorageReplicatedMergeTree` and `ReplicatedMergeTreeRestartingThread`.

void FailPointInjection::pauseFailPoint(const String &)
{
}

void FailPointInjection::notifyPauseAndWaitForResume(const String &)
{
}

void FailPointInjection::disableFailPoint(const String &)
{
}

bool FailPointInjection::hasAnyFailPointBeenRegistered()
{
    return false;
}

/// The rest are only reachable through SYSTEM ... FAILPOINT queries (whose interpreter
/// already throws in builds without libfiu), and pretending to succeed would leave the
/// caller waiting for a fail point that can never fire.

[[noreturn]] static void throwDisabled()
{
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Fail points are disabled because ClickHouse was built without libfiu");
}

void FailPointInjection::enableFailPoint(const String &)
{
    throwDisabled();
}

void FailPointInjection::notifyFailPoint(const String &)
{
    throwDisabled();
}

void FailPointInjection::waitForPause(const String &)
{
    throwDisabled();
}

void FailPointInjection::waitForResume(const String &)
{
    throwDisabled();
}

std::vector<FailPointInjection::FailPointInfo> FailPointInjection::getFailPoints()
{
    std::vector<FailPointInfo> result;

    return result;
}

#endif // USE_LIBFIU

}
