#pragma once
#include <Interpreters/PreparedSets.h>

namespace DB
{

class FutureSet;
using FutureSetPtr = std::shared_ptr<FutureSet>;

/// Wire tag of a serialized set in the plan's sets channel.
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
