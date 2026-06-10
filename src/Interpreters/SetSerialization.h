#pragma once
#include <Interpreters/PreparedSets.h>

namespace DB
{

class FutureSet;
using FutureSetPtr = std::shared_ptr<FutureSet>;

struct SerializedSetsRegistry
{
    struct Hashing
    {
        UInt64 operator()(const FutureSet::Hash & key) const { return key.low64 ^ key.high64; }
    };

    std::unordered_map<FutureSet::Hash, FutureSetPtr, Hashing> sets;

    /// Set when this serialization is used to compute a plan-step cache key (not for transmission).
    /// In that mode `ActionsDAG::serialize` omits the VALUE of non-deterministic constants: such a
    /// value is volatile (e.g. a per-plan-build runtime-filter id) and not a stable key component.
    /// The output is hash-only and never deserialized, so omitting it is safe.
    bool skip_cache_key = false;
};

class ColumnSet;

struct DeserializedSetsRegistry
{
    struct Hashing
    {
        UInt64 operator()(const FutureSet::Hash & key) const { return key.low64 ^ key.high64; }
    };

    std::unordered_map<FutureSet::Hash, std::list<ColumnSet *>, Hashing> sets;
};

}
