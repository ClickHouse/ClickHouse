#pragma once

#include <Parsers/IAST.h>
#include <DataTypes/IDataType.h>
#include <future>
#include <memory>
#include <unordered_map>
#include <vector>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Storages/IStorage_fwd.h>
#include <QueryPipeline/SizeLimits.h>
#include <Processors/QueryPlan/QueryPlan.h>

namespace DB
{

class QueryPlan;

class Set;
using SetPtr = std::shared_ptr<Set>;
class InterpreterSelectWithUnionQuery;

class FutureSet final : public std::shared_future<SetPtr>
{
public:
    FutureSet() = default;
    explicit FutureSet(const std::shared_future<SetPtr> & future) : std::shared_future<SetPtr>(future) {}
    explicit FutureSet(SetPtr readySet);

    bool isReady() const;

    bool isCreated() const;
};

/// Information on how to build set for the [GLOBAL] IN section.
class SubqueryForSet
{
public:

    void createSource(InterpreterSelectWithUnionQuery & interpreter, StoragePtr table_ = nullptr);

    bool hasSource() const;

    /// Returns query plan for the set's source
    /// and removes it from SubqueryForSet because we need to build it only once.
    std::unique_ptr<QueryPlan> detachSource();

    /// Build this set from the result of the subquery.
    String key;
    SetPtr set_in_progress;
    std::promise<SetPtr> promise_to_fill_set;
    FutureSet set = FutureSet{promise_to_fill_set.get_future()};

    /// If set, put the result into the table.
    /// This is a temporary table for transferring to remote servers for distributed query processing.
    StoragePtr table;

    /// The source is obtained using the InterpreterSelectQuery subquery.
    std::unique_ptr<QueryPlan> source;
};

struct PreparedSetKey
{
    /// Prepared sets for tuple literals are indexed by the hash of the tree contents and by the desired
    /// data types of set elements (two different Sets can be required for two tuples with the same contents
    /// if left hand sides of the IN operators have different types).
    static PreparedSetKey forLiteral(const IAST & ast, DataTypes types_);

    /// Prepared sets for subqueries are indexed only by the AST contents because the type of the resulting
    /// set is fully determined by the subquery.
    static PreparedSetKey forSubquery(const IAST & ast);

    IAST::Hash ast_hash;
    DataTypes types; /// Empty for subqueries.

    bool operator==(const PreparedSetKey & other) const;

    struct Hash
    {
        UInt64 operator()(const PreparedSetKey & key) const { return key.ast_hash.first; }
    };
};

inline String toString(const PreparedSetKey & key)
{
    return "__set_" + std::to_string(key.ast_hash.first) + "_" + std::to_string(key.ast_hash.second);
}

class PreparedSets
{
public:
//    explicit PreparedSets(PreparedSetsCachePtr cache_ = {}) : cache(cache_) {}

    using SubqueriesForSets = std::unordered_map<String, SubqueryForSet>;

    SubqueryForSet & createOrGetSubquery(const String & subquery_id, const PreparedSetKey & key,
                                         SizeLimits set_size_limit, bool transform_null_in);
    SubqueryForSet & getSubquery(const String & subquery_id);

    void set(const PreparedSetKey & key, SetPtr set_);
    FutureSet getFuture(const PreparedSetKey & key) const;
    SetPtr get(const PreparedSetKey & key) const;

    /// Get subqueries and clear them.
    /// We need to build a plan for subqueries just once. That's why we can clear them after accessing them.
    /// SetPtr would still be available for consumers of PreparedSets.
    SubqueriesForSets detachSubqueries();

    /// Returns all sets that match the given ast hash not checking types
    /// Used in KeyCondition and MergeTreeIndexConditionBloomFilter to make non exact match for types in PreparedSetKey
    std::vector<SetPtr> getByTreeHash(IAST::Hash ast_hash) const;

    bool empty() const;

private:
    std::unordered_map<PreparedSetKey, FutureSet, PreparedSetKey::Hash> sets;

    /// This is the information required for building sets
    SubqueriesForSets subqueries;
};

using PreparedSetsPtr = std::shared_ptr<PreparedSets>;

/// This set cache is used to avoid building the same set multiple times. It is different from PreparedSets in way that
/// it can be used across multiple queries. One use case is when we execute the same mutation on multiple parts. In this
/// case each part is processed by a separate mutation task but they can share the same set.

/// TODO: need to distinguish between sets with and w/o set_elements!
class PreparedSetsCache
{
public:
    std::variant<std::promise<SetPtr>, FutureSet> findOrPromiseToBuild(const String & key);

private:
    struct Entry
    {
//        std::promise<SetPtr> promise;          /// The promise is set when the set is built by the first task.
        std::shared_future<SetPtr> future; /// Other tasks can wait for the set to be built.
    };

    using EntryPtr = std::shared_ptr<Entry>;

    /// Protects just updates to the cache. When we got EntyPtr from the cache we can access it without locking.
    std::mutex cache_mutex;
    std::unordered_map<String, EntryPtr> cache;
};

using PreparedSetsCachePtr = std::shared_ptr<PreparedSetsCache>;


//FutureSet makeReadyFutureSet(SetPtr set);

}
