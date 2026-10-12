#pragma once

#include <base/defines.h>

#include <condition_variable>
#include <list>
#include <mutex>
#include <string>
#include <unordered_set>

namespace DB
{

/** Exclusive locks over opaque keys, such as the keys of blobs.
  *
  * Two requests conflict when they share a key.
  * A holder acquires all of its keys at once, so there is no lock ordering to maintain and no deadlock.
  * Requests are granted in the order of arrival: a request waits for every earlier request it conflicts with,
  * so a request for many keys eventually gets through an endless stream of small requests.
  */
class KeyLocks
{
public:
    using Keys = std::unordered_set<std::string>;

private:
    using Requests = std::list<Keys>;

public:
    class Guard
    {
    public:
        Guard() = default;
        Guard(Guard && other) noexcept;
        Guard & operator=(Guard && other) noexcept;
        ~Guard();

        Guard(const Guard &) = delete;
        Guard & operator=(const Guard &) = delete;

    private:
        friend class KeyLocks;
        Guard(KeyLocks & locks_, Requests::iterator request_);

        KeyLocks * locks = nullptr;
        Requests::iterator request;
    };

    /// Blocks until none of the earlier requests shares a key with the given ones.
    /// An empty set of keys conflicts with nothing and is granted immediately.
    [[nodiscard]] Guard lock(Keys keys);

private:
    void unlock(Requests::iterator request);

    static bool conflicts(const Keys & lhs, const Keys & rhs);

    std::mutex mutex;
    std::condition_variable released;
    Requests requests TSA_GUARDED_BY(mutex);
};

}
