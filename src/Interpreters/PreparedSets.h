#pragma once

#include <Parsers/IAST.h>
#include <DataTypes/IDataType.h>
#include <memory>
#include <unordered_map>
#include <vector>
#include <future>
#include <Storages/IStorage_fwd.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/SetKeys.h>

namespace DB
{

class QueryPlan;

class Set;
using SetPtr = std::shared_ptr<Set>;
struct SetKeyColumns;

class IQueryTreeNode;
using QueryTreeNodePtr = std::shared_ptr<IQueryTreeNode>;

struct Settings;

/// This is a structure for prepared sets cache.
/// SetPtr can be taken from cache, so we should pass holder for it.
struct SetAndKey
{
    String key;
    SetPtr set;
};

using SetAndKeyPtr = std::shared_ptr<SetAndKey>;

/// Represents a set in a query that might be referenced at analysis time and built later during execution.
/// Also it can represent a constant set that is ready to use.
/// At analysis stage the FutureSets are created but not necessarily filled. Then for non-constant sets there
/// must be an explicit step to build them before they can be used.
/// Set may be useful for indexes, in this case special ordered set with stored elements is build inplace.
class FutureSet
{
public:
    virtual ~FutureSet() = default;

    /// Returns set if set is ready (created and filled) or nullptr if not.
    virtual SetPtr get() const = 0;
    /// Returns set->getElementsTypes(), even if set is not created yet.
    virtual const DataTypes & getTypes() const = 0;
    /// If possible, return set with stored elements useful for PK analysis.
    virtual SetPtr buildOrderedSetInplace(const ContextPtr & context) = 0;
};

using FutureSetPtr = std::shared_ptr<FutureSet>;

/// Future set from already filled set.
/// Usually it is from StorageSet.
class FutureSetFromStorage final : public FutureSet
{
public:
    FutureSetFromStorage(SetPtr set_);

    SetPtr get() const override;
    const DataTypes & getTypes() const override;
    SetPtr buildOrderedSetInplace(const ContextPtr &) override;

private:
    SetPtr set;
};

/// Set from tuple is filled as well as set from storage.
/// Additionally, it can be converted to set useful for PK.
class FutureSetFromTuple final : public FutureSet
{
public:
    FutureSetFromTuple(Block block, const Settings & settings);

    SetPtr get() const override { return set; }
    SetPtr buildOrderedSetInplace(const ContextPtr & context) override;

    const DataTypes & getTypes() const override;

private:
    SetPtr set;
    SetKeyColumns set_key_columns;
};

/// Set from subquery can be built inplace for PK or in CreatingSet step.
/// If use_index_for_in_with_subqueries_max_values is reached, set for PK won't be created,
/// but ordinary set would be created instead.
class FutureSetFromSubquery final : public FutureSet
{
public:
    FutureSetFromSubquery(
        String key,
        std::unique_ptr<QueryPlan> source_,
        StoragePtr external_table_,
        FutureSetPtr external_table_set_,
        const Settings & settings);

    FutureSetFromSubquery(
        String key,
        QueryTreeNodePtr query_tree_,
        const Settings & settings);

    SetPtr get() const override;
    const DataTypes & getTypes() const override;
    SetPtr buildOrderedSetInplace(const ContextPtr & context) override;

    std::unique_ptr<QueryPlan> build(const ContextPtr & context);

    QueryTreeNodePtr detachQueryTree() { return std::move(query_tree); }
    void setQueryPlan(std::unique_ptr<QueryPlan> source_);

private:
    SetAndKeyPtr set_and_key;
    StoragePtr external_table;
    FutureSetPtr external_table_set;

    std::unique_ptr<QueryPlan> source;
    QueryTreeNodePtr query_tree;
};

/// Container for all the sets used in query.
class PreparedSets
{
public:

    using Hash = CityHash_v1_0_2::uint128;
    struct Hashing
    {
        UInt64 operator()(const Hash & key) const { return key.low64 ^ key.high64; }
    };

    using SetsFromTuple = std::unordered_map<Hash, std::vector<std::shared_ptr<FutureSetFromTuple>>, Hashing>;
    using SetsFromStorage = std::unordered_map<Hash, std::shared_ptr<FutureSetFromStorage>, Hashing>;
    using SetsFromSubqueries = std::unordered_map<Hash, std::shared_ptr<FutureSetFromSubquery>, Hashing>;

    FutureSetPtr addFromStorage(const Hash & key, SetPtr set_);
    FutureSetPtr addFromTuple(const Hash & key, Block block, const Settings & settings);

    FutureSetPtr addFromSubquery(
        const Hash & key,
        std::unique_ptr<QueryPlan> source,
        StoragePtr external_table,
        FutureSetPtr external_table_set,
        const Settings & settings);

    FutureSetPtr addFromSubquery(
        const Hash & key,
        QueryTreeNodePtr query_tree,
        const Settings & settings);

    FutureSetPtr findTuple(const Hash & key, const DataTypes & types) const;
    std::shared_ptr<FutureSetFromStorage> findStorage(const Hash & key) const;
    std::shared_ptr<FutureSetFromSubquery> findSubquery(const Hash & key) const;

    using Subqueries = std::vector<std::shared_ptr<FutureSetFromSubquery>>;
    Subqueries getSubqueries();

    const SetsFromTuple & getSetsFromTuple() const { return sets_from_tuple; }
    // const SetsFromStorage & getSetsFromStorage() const { return sets_from_storage; }
    // const SetsFromSubqueries & getSetsFromSubquery() const { return sets_from_subqueries; }

    static String toString(const Hash & key, const DataTypes & types);

private:
    SetsFromTuple sets_from_tuple;
    SetsFromStorage sets_from_storage;
    SetsFromSubqueries sets_from_subqueries;
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
