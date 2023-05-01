#include <Common/Exception.h>
#include <Common/FailPoint.h>

#include <boost/core/noncopyable.hpp>
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

#define APPLY_FOR_FAILPOINTS_ONCE(M) \
    M(rmt_commit_zk_fail_after_op)

#define APPLY_FOR_FAILPOINTS(M) \
    M(dummy_failpoint)

#define APPLY_FOR_PAUSEABLE_FAILPOINTS_ONCE(M) \
    M(dummy_pausable_failpoint_once)

#define APPLY_FOR_PAUSEABLE_FAILPOINTS(M) \
    M(dummy_pausable_failpoint)

namespace FailPoints
{
#define M(NAME) extern const char(NAME)[] = #NAME "";
APPLY_FOR_FAILPOINTS_ONCE(M)
APPLY_FOR_FAILPOINTS(M)
APPLY_FOR_PAUSEABLE_FAILPOINTS_ONCE(M)
APPLY_FOR_PAUSEABLE_FAILPOINTS(M)
#undef M
}

std::unordered_map<String, std::any> FailPointHelper::fail_point_val;
std::unordered_map<String, std::shared_ptr<FailPointChannel>> FailPointHelper::fail_point_wait_channels;
class FailPointChannel : private boost::noncopyable
{
public:
    // wake up all waiting threads when destroy
    ~FailPointChannel() { notifyAll(); }

    explicit FailPointChannel(UInt64 timeout_)
        : timeout(timeout_)
    {}
    FailPointChannel()
        : timeout(0)
    {}

    void wait()
    {
        std::unique_lock lock(m);
        if (timeout == 0)
            cv.wait(lock);
        else
            cv.wait_for(lock, std::chrono::seconds(timeout));
    }

    void notifyAll()
    {
        std::unique_lock lock(m);
        cv.notify_all();
    }

private:
    UInt64 timeout;
    std::mutex m;
    std::condition_variable cv;
};

void FailPointHelper::enablePauseFailPoint(const String & fail_point_name, UInt64 time)
{
#define SUB_M(NAME, flags)                                                                                  \
    if (fail_point_name == FailPoints::NAME)                                                                \
    {                                                                                                       \
        /* FIU_ONETIME -- Only fail once; the point of failure will be automatically disabled afterwards.*/ \
        fiu_enable(FailPoints::NAME, 1, nullptr, flags);                                                    \
        fail_point_wait_channels.try_emplace(FailPoints::NAME, std::make_shared<FailPointChannel>(time));   \
        return;                                                                                             \
    }

#define M(NAME) SUB_M(NAME, FIU_ONETIME)
    APPLY_FOR_PAUSEABLE_FAILPOINTS_ONCE(M)
#undef M

#define M(NAME) SUB_M(NAME, 0)
    APPLY_FOR_PAUSEABLE_FAILPOINTS(M)
#undef M
#undef SUB_M

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot find fail point {}", fail_point_name);
}

void FailPointHelper::enableFailPoint(const String & fail_point_name, std::optional<std::any> v)
{
#if FIU_ENABLE
#define SUB_M(NAME, flags)                                                                                  \
    if (fail_point_name == FailPoints::NAME)                                                                \
    {                                                                                                       \
        /* FIU_ONETIME -- Only fail once; the point of failure will be automatically disabled afterwards.*/ \
        fiu_enable(FailPoints::NAME, 1, nullptr, flags);                                                    \
        if (v.has_value())                                                                                  \
        {                                                                                                   \
            fail_point_val.try_emplace(FailPoints::NAME, v.value());                                        \
        }                                                                                                   \
        return;                                                                                             \
    }

#define M(NAME) SUB_M(NAME, FIU_ONETIME)
    APPLY_FOR_FAILPOINTS_ONCE(M)
#undef M
#define M(NAME) SUB_M(NAME, 0)
    APPLY_FOR_FAILPOINTS(M)
#undef M
#undef SUB_M

#define SUB_M(NAME, flags)                                                                                  \
    if (fail_point_name == FailPoints::NAME)                                                                \
    {                                                                                                       \
        /* FIU_ONETIME -- Only fail once; the point of failure will be automatically disabled afterwards.*/ \
        fiu_enable(FailPoints::NAME, 1, nullptr, flags);                                                    \
        fail_point_wait_channels.try_emplace(FailPoints::NAME, std::make_shared<FailPointChannel>());       \
        if (v.has_value())                                                                                  \
        {                                                                                                   \
            fail_point_val.try_emplace(FailPoints::NAME, v.value());                                        \
        }                                                                                                   \
        return;                                                                                             \
    }

#define M(NAME) SUB_M(NAME, FIU_ONETIME)
    APPLY_FOR_PAUSEABLE_FAILPOINTS_ONCE(M)
#undef M

#define M(NAME) SUB_M(NAME, 0)
    APPLY_FOR_PAUSEABLE_FAILPOINTS(M)
#undef M
#undef SUB_M

#endif
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot find fail point {}", fail_point_name);
}

std::optional<std::any>
FailPointHelper::getFailPointVal(const String & fail_point_name)
{
    if (auto iter = fail_point_val.find(fail_point_name); iter != fail_point_val.end())
    {
        return iter->second;
    }
    return std::nullopt;
}

void FailPointHelper::disableFailPoint(const String & fail_point_name)
{
    if (auto iter = fail_point_wait_channels.find(fail_point_name); iter != fail_point_wait_channels.end())
    {
        /// can not rely on deconstruction to do the notify_all things, because
        /// if someone wait on this, the deconstruct will never be called.
        iter->second->notifyAll();
        fail_point_wait_channels.erase(iter);
    }
    fail_point_val.erase(fail_point_name);
    fiu_disable(fail_point_name.c_str());
}

void FailPointHelper::wait(const String & fail_point_name)
{
    if (auto iter = fail_point_wait_channels.find(fail_point_name); iter == fail_point_wait_channels.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can not find channel for fail point {}", fail_point_name);
    else
    {
        auto ptr = iter->second;
        ptr->wait();
    }
}

}
