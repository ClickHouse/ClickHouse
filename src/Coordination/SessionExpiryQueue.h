#pragma once
#include <unordered_map>
#include <unordered_set>
#include <chrono>

namespace DB
{

class SessionExpiryQueue
{
private:
    std::unordered_map<int64_t, int64_t> session_to_timeout;
    std::unordered_map<int64_t, std::unordered_set<int64_t>> expiry_to_sessions;

    int64_t expiration_interval;
    int64_t next_expiration_time;

    static int64_t getNowMilliseconds()
    {
        using namespace std::chrono;
        return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    }

    int64_t roundToNextInterval(int64_t time) const
    {
        return (time / expiration_interval + 1) * expiration_interval;
    }

public:
    explicit SessionExpiryQueue(int64_t expiration_interval_)
        : expiration_interval(expiration_interval_)
        , next_expiration_time(roundToNextInterval(getNowMilliseconds()))
    {
    }

    bool remove(int64_t session_id);

    bool update(int64_t session_id, int64_t timeout_ms);

    std::unordered_set<int64_t> getExpiredSessions();

    void clear();
};

}
