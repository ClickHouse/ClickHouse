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
#include "Core/Block.h"
#include "Interpreters/Context.h"
#include "Interpreters/Set.h"
#include "Processors/Executors/CompletedPipelineExecutor.h"
#include "Processors/QueryPlan/BuildQueryPipelineSettings.h"
#include "Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h"
#include "Processors/Sinks/EmptySink.h"
#include "Processors/Sinks/NullSink.h"
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

class QueryPlan;

class Set;
using SetPtr = std::shared_ptr<Set>;
class InterpreterSelectWithUnionQuery;

class IQueryTreeNode;
using QueryTreeNodePtr = std::shared_ptr<IQueryTreeNode>;

/// Represents a set in a query that might be referenced at analysis time and built later during execution.
/// Also it can represent a constant set that is ready to use.
/// At analysis stage the FutureSets are created but not necessarily filled. Then for non-constant sets there
/// must be an explicit step to build them before they can be used.
/// FutureSet objects can be stored in PreparedSets and are not intended to be used from multiple threads.
// class FutureSet final
// {
// public:
//     FutureSet() = default;

//     /// Create FutureSet from an object that will be created in the future.
//     explicit FutureSet(const std::shared_future<SetPtr> & future_set_) : future_set(future_set_) {}

//     /// Create FutureSet from a ready set.
//     explicit FutureSet(SetPtr readySet);

//     /// The set object will be ready in the future, as opposed to 'null' object  when FutureSet is default constructed.
//     bool isValid() const { return future_set.valid(); }

//     /// The the value of SetPtr is ready, but the set object might not have been filled yet.
//     bool isReady() const;

//     /// The set object is ready and filled.
//     bool isCreated() const;

//     SetPtr get() const { chassert(isReady()); return future_set.get(); }

// private:
//     std::shared_future<SetPtr> future_set;
// };

class FutureSet
{
public:
    virtual ~FutureSet() = default;

    virtual bool isReady() const = 0;
    virtual bool isFilled() const = 0;
    virtual SetPtr get() const = 0;

    virtual SetPtr buildOrderedSetInplace(const ContextPtr & context) = 0;
    virtual std::unique_ptr<QueryPlan> build(const ContextPtr & context) = 0;

    virtual DataTypes getTypes() const = 0;

    // static SizeLimits getSizeLimitsForSet(const Settings & settings, bool ordered_set);
};

using FutureSetPtr = std::shared_ptr<FutureSet>;

class FutureSetFromTuple final : public FutureSet
{
public:
    FutureSetFromTuple(Block block, const Settings & settings);

    bool isReady() const override { return true; }
    bool isFilled() const override { return true; }
    SetPtr get() const override { return set; }

    SetPtr buildOrderedSetInplace(const ContextPtr & context) override;

    std::unique_ptr<QueryPlan> build(const ContextPtr &) override;

    DataTypes getTypes() const override { return set->getElementsTypes(); }

///    void buildForTuple(SizeLimits size_limits, bool transform_null_in);

private:
    SetPtr set;
    Set::SetKeyColumns set_key_columns;

    //void fill(SizeLimits size_limits, bool transform_null_in, bool create_ordered_set);
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
    SetPtr set;
    /// After set_in_progress is finished it will be put into promise_to_fill_set and thus all FutureSet's
    /// that are referencing this set will be filled.

    std::promise<SetPtr> promise_to_fill_set;
    // FutureSet set = FutureSet{promise_to_fill_set.get_future()};

    /// If set, put the result into the table.
    /// This is a temporary table for transferring to remote servers for distributed query processing.
    StoragePtr table;

    /// The source is obtained using the InterpreterSelectQuery subquery.
    std::unique_ptr<QueryPlan> source;
    QueryTreeNodePtr query_tree;
};

class FutureSetFromSubquery : public FutureSet, public std::enable_shared_from_this<FutureSetFromSubquery>
{
public:
    FutureSetFromSubquery(SubqueryForSet subquery_, FutureSetPtr external_table_set_, bool transform_null_in_);

    bool isReady() const override { return subquery.set != nullptr && subquery.set->isCreated(); }
    bool isFilled() const override { return isReady(); }
    SetPtr get() const override { return subquery.set; }

    SetPtr buildOrderedSetInplace(const ContextPtr & context) override;

    std::unique_ptr<QueryPlan> build(const ContextPtr & context) override
    {
        return buildPlan(context, false);
    }

    DataTypes getTypes() const override;

    // void addStorage(StoragePtr storage) { subquery.table = std::move(storage); }

    SubqueryForSet & getSubquery() { return subquery; }

private:
    //SetPtr set;
    SubqueryForSet subquery;
    FutureSetPtr external_table_set;
    bool transform_null_in;

    std::unique_ptr<QueryPlan> buildPlan(const ContextPtr & context, bool create_ordered_set);
};

class FutureSetFromStorage : public FutureSet
{
public:
    FutureSetFromStorage(SetPtr set_); // : set(std::move(set_) {}

    bool isReady() const override { return set != nullptr; }
    bool isFilled() const override { return isReady(); }
    SetPtr get() const override { return set; }

    SetPtr buildOrderedSetInplace(const ContextPtr &) override
    {
        return set->hasExplicitSetElements() ? set : nullptr;
    }

    DataTypes getTypes() const override { return set->getElementsTypes(); }

    std::unique_ptr<QueryPlan> build(const ContextPtr &) override { return nullptr; }

private:
    SetPtr set;
};

// class FutureSetFromFuture : public FutureSet
// {
// public:
//     FutureSetFromFuture(std::shared_future<SetPtr> future_set_);

//     bool isReady() const override { return future_set.wait_for(std::chrono::seconds(0)) == std::future_status::ready; }
//     SetPtr get() const override { return future_set.get(); }

//     SetPtr buildOrderedSetInplace(const ContextPtr &) override
//     {
//         fill(true);
//         return set;
//     }

//     std::unique_ptr<QueryPlan> build(const ContextPtr &) override
//     {
//         fill(false);
//         return nullptr;
//     }

// private:
//     std::shared_future<SetPtr> future_set;
// }

struct PreparedSetKey
{
    using Hash = std::pair<UInt64, UInt64>;

    /// Prepared sets for tuple literals are indexed by the hash of the tree contents and by the desired
    /// data types of set elements (two different Sets can be required for two tuples with the same contents
    /// if left hand sides of the IN operators have different types).
    static PreparedSetKey forLiteral(Hash hash, DataTypes types_);

    /// Prepared sets for subqueries are indexed only by the AST contents because the type of the resulting
    /// set is fully determined by the subquery.
    static PreparedSetKey forSubquery(Hash hash);

    Hash ast_hash;
    DataTypes types; /// Empty for subqueries.

    bool operator==(const PreparedSetKey & other) const;

    String toString() const;

    struct Hashing
    {
        UInt64 operator()(const PreparedSetKey & key) const { return key.ast_hash.first; }
    };
};

class PreparedSets
{
public:
    struct SetAndName
    {
        String name;
        std::shared_ptr<FutureSetFromSubquery> set;
    };
    using SubqueriesForSets = std::vector<SetAndName>;

    // SubqueryForSet & createOrGetSubquery(const String & subquery_id, const PreparedSetKey & key,
    //                                      SizeLimits set_size_limit, bool transform_null_in);

    FutureSetPtr addFromStorage(const PreparedSetKey & key, SetPtr set_);
    FutureSetPtr addFromTuple(const PreparedSetKey & key, Block block, const Settings & settings);
    FutureSetPtr addFromSubquery(const PreparedSetKey & key, SubqueryForSet subquery, const Settings & settings, FutureSetPtr external_table_set);

    //void addStorageToSubquery(const String & subquery_id, StoragePtr external_storage);

    FutureSetPtr getFuture(const PreparedSetKey & key) const;
    //SubqueryForSet & getSubquery(const String & subquery_id);
    // SetPtr get(const PreparedSetKey & key) const;

    /// Get subqueries and clear them.
    /// We need to build a plan for subqueries just once. That's why we can clear them after accessing them.
    /// SetPtr would still be available for consumers of PreparedSets.
    SubqueriesForSets detachSubqueries();

    /// Returns all sets that match the given ast hash not checking types
    /// Used in KeyCondition and MergeTreeIndexConditionBloomFilter to make non exact match for types in PreparedSetKey
    //std::vector<FutureSetPtr> getByTreeHash(IAST::Hash ast_hash) const;

    const std::unordered_map<PreparedSetKey, FutureSetPtr, PreparedSetKey::Hashing> & getSets() const { return sets; }

    bool empty() const;

private:
    std::unordered_map<PreparedSetKey, FutureSetPtr, PreparedSetKey::Hashing> sets;

    /// This is the information required for building sets
    SubqueriesForSets subqueries;
};

using PreparedSetsPtr = std::shared_ptr<PreparedSets>;

/// A reference to a set that is being built by another task.
/// The difference from FutureSet is that this object can be used to wait for the set to be built in another thread.
using SharedSet = std::shared_future<SetPtr>;

/// This set cache is used to avoid building the same set multiple times. It is different from PreparedSets in way that
/// it can be used across multiple queries. One use case is when we execute the same mutation on multiple parts. In this
/// case each part is processed by a separate mutation task but they can share the same set.
class PreparedSetsCache
{
public:
    /// Lookup for set in the cache.
    /// If it is found, get the future to be able to wait for the set to be built.
    /// Otherwise create a promise, build the set and set the promise value.
    std::variant<std::promise<SetPtr>, SharedSet> findOrPromiseToBuild(const String & key);

private:
    struct Entry
    {
        SharedSet future; /// Other tasks can wait for the set to be built.
    };

    std::mutex cache_mutex;
    std::unordered_map<String, Entry> cache;
};

using PreparedSetsCachePtr = std::shared_ptr<PreparedSetsCache>;

}
