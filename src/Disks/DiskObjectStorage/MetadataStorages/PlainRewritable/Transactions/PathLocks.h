#pragma once

#include <base/defines.h>

#include <condition_variable>
#include <list>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

namespace DB
{

/** Hierarchical locks over normalized paths of the virtual filesystem.
  *
  * Two paths conflict when they are equal or when one of them is an ancestor of the other;
  * the empty path denotes the root and conflicts with everything.
  * A holder acquires all of its paths at once, so there is no lock ordering to maintain and no deadlock.
  * Requests are granted in the order of arrival: a request waits for every earlier request it conflicts with,
  * which guarantees that a request for the root eventually gets through an endless stream of small requests.
  */
class PathLocks
{
    struct Request
    {
        std::vector<std::string> paths;
    };

    using Requests = std::list<Request>;

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
        friend class PathLocks;
        Guard(PathLocks & locks_, Requests::iterator request_);

        PathLocks * locks = nullptr;
        Requests::iterator request;
    };

    /// Blocks until none of the earlier requests conflicts with any of the given paths.
    [[nodiscard]] Guard lock(std::vector<std::string> paths);

    /// Blocks until no other request is held, then excludes all other requests until the guard is released.
    /// Later requests wait for it, so it should be used only when the caller cannot proceed otherwise.
    [[nodiscard]] Guard lockAll();

    /// The same as `lockAll`, but does not wait: returns nothing if any other request is held or waiting.
    [[nodiscard]] std::optional<Guard> tryLockAll();

private:
    void unlock(Requests::iterator request);

    static bool conflicts(const std::string & lhs, const std::string & rhs);
    static bool conflicts(const Request & lhs, const Request & rhs);

    std::mutex mutex;
    std::condition_variable released;
    Requests requests TSA_GUARDED_BY(mutex);
};

}
