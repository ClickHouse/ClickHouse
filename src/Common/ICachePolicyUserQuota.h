#pragma once

#include <base/UUID.h>
#include <base/types.h>

namespace DB
{

/// Per-user quotas for usage of shared caches, used by ICachePolicy.
/// Currently allows to limit
/// - the maximum amount of cache memory a user may consume
/// - the maximum number of items a user can store in the cache
/// Note that caches usually also have global limits which restrict these values at cache level. Per-user quotas have no effect if they
/// exceed the global thresholds.
class ICachePolicyUserQuota
{
public:
    /// Register or update the user's quota for the given resource.
    virtual void setQuotaForUser(const UUID & user_id, size_t max_size_in_bytes, size_t max_entries) = 0;

    /// Update the actual resource usage for the given user.
    virtual void increaseActual(const UUID & user_id, size_t entry_size_in_bytes) = 0;
    virtual void decreaseActual(const UUID & user_id, size_t entry_size_in_bytes) = 0;

    /// Is the user allowed to write a new entry into the cache?
    virtual bool approveWrite(const UUID & user_id, size_t entry_size_in_bytes) const = 0;

    /// Clears the policy contents
    virtual void clear() = 0;

    virtual ~ICachePolicyUserQuota() = default;
};

using CachePolicyUserQuotaPtr = std::unique_ptr<ICachePolicyUserQuota>;


class NoCachePolicyUserQuota : public ICachePolicyUserQuota
{
public:
    void setQuotaForUser(const UUID & /*user_id*/, size_t /*max_size_in_bytes*/, size_t /*max_entries*/) override {}
    void increaseActual(const UUID & /*user_id*/, size_t /*entry_size_in_bytes*/) override {}
    void decreaseActual(const UUID & /*user_id*/, size_t /*entry_size_in_bytes*/) override {}
    bool approveWrite(const UUID & /*user_id*/, size_t /*entry_size_in_bytes*/) const override { return true; }
    void clear() override {}
};


}
