#pragma once

#include <utility>

namespace DB
{

/// Marks synchronous rollback/deletion work, without suppressing operation cancellation.
/// S3 may give each cleanup delete one transport attempt after cancellation. Reads/writes,
/// retries, redirects and retry backoff remain cancelled; normal network timeouts still apply.
/// This permission is thread-local and is deliberately not inherited by unrelated workers.
class RemoteIOCleanupScope
{
public:
    RemoteIOCleanupScope() noexcept : previous(std::exchange(active, true))
    {
    }

    ~RemoteIOCleanupScope()
    {
        active = previous;
    }

    RemoteIOCleanupScope(const RemoteIOCleanupScope &) = delete;
    RemoteIOCleanupScope & operator=(const RemoteIOCleanupScope &) = delete;

    static bool isActive() { return active; }

private:
    inline static thread_local bool active = false;
    const bool previous;
};

}
