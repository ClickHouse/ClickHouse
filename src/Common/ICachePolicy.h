#pragma once

#include <Common/Exception.h>
#include <Common/ICachePolicyUserQuota.h>

#include <functional>
#include <memory>
#include <mutex>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

template <typename T>
struct EqualWeightFunction
{
    size_t operator()(const T &) const
    {
        return 1;
    }
};

template <typename TKey, typename TMapped, typename HashFunction = std::hash<TKey>, typename WeightFunction = EqualWeightFunction<TMapped>>
class ICachePolicy
{
public:
    using Key = TKey;
    using Mapped = TMapped;
    using MappedPtr = std::shared_ptr<Mapped>;
    using OnWeightLossFunction = std::function<void(size_t)>;

    struct KeyMapped
    {
        Key key;
        MappedPtr mapped;
    };

    explicit ICachePolicy(CachePolicyUserQuotaPtr user_quotas_) : user_quotas(std::move(user_quotas_)) {}
    virtual ~ICachePolicy() = default;

    virtual size_t weight(std::lock_guard<std::mutex> & /*cache_lock*/) const = 0;
    virtual size_t count(std::lock_guard<std::mutex> & /*cache_lock*/) const = 0;
    virtual size_t maxSize(std::lock_guard<std::mutex>& /*cache_lock*/) const = 0;

    virtual void setMaxCount(size_t /*max_count*/, std::lock_guard<std::mutex> & /* cache_lock */) { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for cache policy"); }
    virtual void setMaxSize(size_t /*max_size_in_bytes*/, std::lock_guard<std::mutex> & /* cache_lock */) { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented for cache policy"); }
    virtual void setQuotaForUser(const String & user_name, size_t max_size_in_bytes, size_t max_entries, std::lock_guard<std::mutex> & /*cache_lock*/) { user_quotas->setQuotaForUser(user_name, max_size_in_bytes, max_entries); }

    /// HashFunction usually hashes the entire key and the found key will be equal the provided key. In such cases, use get(). It is also
    /// possible to store other, non-hashed data in the key. In that case, the found key is potentially different from the provided key.
    /// Then use getWithKey() to also return the found key including it's non-hashed data.
    virtual MappedPtr get(const Key & key, std::lock_guard<std::mutex> & /* cache_lock */) = 0;
    virtual std::optional<KeyMapped> getWithKey(const Key &, std::lock_guard<std::mutex> & /*cache_lock*/) = 0;

    virtual void set(const Key & key, const MappedPtr & mapped, std::lock_guard<std::mutex> & /*cache_lock*/) = 0;

    virtual void remove(const Key & key, std::lock_guard<std::mutex> & /*cache_lock*/) = 0;

    virtual void reset(std::lock_guard<std::mutex> & /*cache_lock*/) = 0;
    virtual std::vector<KeyMapped> dump() const = 0;

protected:
    CachePolicyUserQuotaPtr user_quotas;
};

}
