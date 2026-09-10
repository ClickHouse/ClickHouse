#pragma once
#include <Columns/ColumnSet.h>
#include <Interpreters/PreparedSets.h>

#include <list>

namespace DB
{

class FutureSet;
using FutureSetPtr = std::shared_ptr<FutureSet>;

/// What kind of set this is, as written in the part of a serialized plan that carries its sets.
enum class SetSerializationKind : UInt8
{
    StorageSet = 1,
    TupleValues = 2,
    SubqueryPlan = 3,
};

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

    /// Entries sorted by hash. The map's iteration order is not stable across builds, and plan
    /// bytes are hashed for identity (e.g. `sipHash64` of a task's serialized plan), so anything
    /// written to the wire must use this fixed order.
    std::vector<std::pair<FutureSet::Hash, FutureSet *>> entriesSortedByHash() const;
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
