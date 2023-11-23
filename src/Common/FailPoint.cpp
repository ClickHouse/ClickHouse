#include <Common/Exception.h>
#include <Common/FailPoint.h>

#include <boost/core/noncopyable.hpp>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <optional>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
};

#if FIU_ENABLE
static struct InitFiu
{
    InitFiu()
    {
        fiu_init(0);
    }
} init_fiu;
#endif

/// We should define different types of failpoints here. There are four types of them:
/// - ONCE: the failpoint will only be triggered once.
/// - REGULAR: the failpoint will always be triggered util disableFailPoint is called.
/// - PAUSAEBLE_ONCE: the failpoint will be blocked one time when pauseFailPoint is called, util disableFailPoint is called.
/// - PAUSAEBLE: the failpoint will be blocked every time when pauseFailPoint is called, util disableFailPoint is called.

#define APPLY_FOR_FAILPOINTS(ONCE, REGULAR, PAUSEABLE_ONCE, PAUSEABLE) \
    ONCE(replicated_merge_tree_commit_zk_fail_after_op) \
    ONCE(replicated_merge_tree_insert_quorum_fail_0) \
    REGULAR(use_delayed_remote_source) \
    REGULAR(dummy_failpoint) \
    PAUSEABLE_ONCE(dummy_pausable_failpoint_once) \
    PAUSEABLE(dummy_pausable_failpoint)

namespace FailPoints
{
#define M(NAME) extern const char(NAME)[] = #NAME "";
APPLY_FOR_FAILPOINTS(M, M, M, M)
#undef M
}

std::unordered_map<String, std::shared_ptr<FailPointChannel>> FailPointInjection::fail_point_wait_channels;
std::mutex FailPointInjection::mu;
class FailPointChannel : private boost::noncopyable
{
public:
    explicit FailPointChannel(UInt64 timeout_)
        : timeout_ms(timeout_)
    {}
    FailPointChannel()
        : timeout_ms(0)
    {}

    void wait()
    {
        std::unique_lock lock(m);
        if (timeout_ms == 0)
            cv.wait(lock);
        else
            cv.wait_for(lock, std::chrono::milliseconds(timeout_ms));
    }

    void notifyAll()
    {
        std::unique_lock lock(m);
        cv.notify_all();
    }

private:
    UInt64 timeout_ms;
    std::mutex m;
    std::condition_variable cv;
};

void FailPointInjection::enablePauseFailPoint(const String & fail_point_name, UInt64 time_ms)
{
#define SUB_M(NAME, flags)                                                                                  \
    if (fail_point_name == FailPoints::NAME)                                                                \
    {                                                                                                       \
        /* FIU_ONETIME -- Only fail once; the point of failure will be automatically disabled afterwards.*/ \
        fiu_enable(FailPoints::NAME, 1, nullptr, flags);                                                    \
        std::lock_guard lock(mu);                                                                           \
        fail_point_wait_channels.try_emplace(FailPoints::NAME, std::make_shared<FailPointChannel>(time_ms));   \
        return;                                                                                             \
    }
#define ONCE(NAME)
#define REGULAR(NAME)
#define PAUSEABLE_ONCE(NAME) SUB_M(NAME, FIU_ONETIME)
#define PAUSEABLE(NAME) SUB_M(NAME, 0)
    APPLY_FOR_FAILPOINTS(ONCE, REGULAR, PAUSEABLE_ONCE, PAUSEABLE)
#undef SUB_M
#undef ONCE
#undef REGULAR
#undef PAUSEABLE_ONCE
#undef PAUSEABLE

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot find fail point {}", fail_point_name);
}

void FailPointInjection::pauseFailPoint(const String & fail_point_name)
{
    fiu_do_on(fail_point_name.c_str(), FailPointInjection::wait(fail_point_name););
}

void FailPointInjection::enableFailPoint(const String & fail_point_name)
{
#if FIU_ENABLE
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

#endif
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot find fail point {}", fail_point_name);
}

void FailPointInjection::disableFailPoint(const String & fail_point_name)
{
    std::lock_guard lock(mu);
    if (auto iter = fail_point_wait_channels.find(fail_point_name); iter != fail_point_wait_channels.end())
    {
        /// can not rely on deconstruction to do the notify_all things, because
        /// if someone wait on this, the deconstruct will never be called.
        iter->second->notifyAll();
        fail_point_wait_channels.erase(iter);
    }
    fiu_disable(fail_point_name.c_str());
}

void FailPointInjection::wait(const String & fail_point_name)
{
    std::unique_lock lock(mu);
    if (auto iter = fail_point_wait_channels.find(fail_point_name); iter == fail_point_wait_channels.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can not find channel for fail point {}", fail_point_name);
    else
    {
        lock.unlock();
        auto ptr = iter->second;
        ptr->wait();
    }
};

}
