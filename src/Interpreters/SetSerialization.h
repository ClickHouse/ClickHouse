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
    /// MUST be kept in sync with `for_cache_key` on `IQueryPlanStep::Serialization`: this one drives
    /// `ActionsDAG::serialize` (skips the runtime-filter id value), that one drives the step's own
    /// `serialize`. Set both or neither.
    bool for_cache_key = false;
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
