#pragma once
#include <unordered_map>
#include <unordered_set>
#include <chrono>

namespace DB
{

class SessionExpiryQueue
{
private:
    std::unordered_map<int64_t, long> session_to_timeout;
    std::unordered_map<long, std::unordered_set<int64_t>> expiry_to_sessions;

    long expiration_interval;
    long next_expiration_time;

    static long getNowMilliseconds()
    {
        using namespace std::chrono;
        return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    }

    long roundToNextInterval(long time) const
    {
        return (time / expiration_interval + 1) * expiration_interval;
    }

public:
    explicit SessionExpiryQueue(long expiration_interval_)
        : expiration_interval(expiration_interval_)
        , next_expiration_time(roundToNextInterval(getNowMilliseconds()))
    {
    }

    bool remove(int64_t session_id);

    bool update(int64_t session_id, long timeout_ms);

    std::unordered_set<int64_t> getExpiredSessions();
};

}
