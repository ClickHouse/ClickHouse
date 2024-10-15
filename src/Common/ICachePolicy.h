#pragma once

#include <Common/Exception.h>
#include <Common/ICachePolicyUserQuota.h>
#include <base/UUID.h>

#include <functional>
#include <memory>
#include <optional>

namespace DB
{

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

    virtual size_t sizeInBytes() const = 0;
    virtual size_t count() const = 0;
    virtual size_t maxSizeInBytes() const = 0;

    virtual void setMaxCount(size_t /*max_count*/) = 0;
    virtual void setMaxSizeInBytes(size_t /*max_size_in_bytes*/) = 0;
    virtual void setQuotaForUser(const UUID & user_id, size_t max_size_in_bytes, size_t max_entries) { user_quotas->setQuotaForUser(user_id, max_size_in_bytes, max_entries); }

    /// HashFunction usually hashes the entire key and the found key will be equal the provided key. In such cases, use get(). It is also
    /// possible to store other, non-hashed data in the key. In that case, the found key is potentially different from the provided key.
    /// Then use getWithKey() to also return the found key including its non-hashed data.
    virtual MappedPtr get(const Key & key) = 0;
    virtual std::optional<KeyMapped> getWithKey(const Key &) = 0;

    virtual void set(const Key & key, const MappedPtr & mapped) = 0;

    virtual void remove(const Key & key) = 0;
    virtual void remove(std::function<bool(const Key & key, const MappedPtr & mapped)> predicate) = 0;

    virtual void clear() = 0;
    virtual std::vector<KeyMapped> dump() const = 0;

protected:
    CachePolicyUserQuotaPtr user_quotas;
};

}
