#pragma once

#include <Core/Types.h>
#include <Common/CacheBase.h>
#include <Common/CurrentMetrics.h>
#include <Common/PODArray.h>
#include <Common/SipHash.h>

namespace DB
{

struct CacheKey
{
    UInt128 domain_id;
    UInt128 value_hash;

    bool operator==(const CacheKey & rhs) const { return domain_id == rhs.domain_id && value_hash == rhs.value_hash; }
};

struct CacheKeyHash
{
    size_t operator()(const CacheKey & key) const noexcept
    {
        SipHash sip;
        sip.update(key.domain_id);
        sip.update(key.value_hash);
        return static_cast<size_t>(sip.get64());
    }
};

using SerializedKeys = PODArray<UInt8>;

using SerializedKeysPtr = std::shared_ptr<SerializedKeys>;

struct SerializedKeysWeight
{
    size_t operator()(const SerializedKeys & mapped) const { return mapped.capacity() + sizeof(SerializedKeys); }
};


class ReverseLookupCache : public CacheBase<CacheKey, SerializedKeys, CacheKeyHash, SerializedKeysWeight>
{
public:
    using Base = CacheBase<CacheKey, SerializedKeys, CacheKeyHash, SerializedKeysWeight>;
    using Base::Base;
};

}
