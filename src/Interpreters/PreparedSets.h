#pragma once

#include <Parsers/IAST.h>
#include <DataTypes/IDataType.h>
#include <memory>
#include <unordered_map>
#include <vector>
#include <future>
#include <Storages/IStorage_fwd.h>
#include <Interpreters/Context_fwd.h>
#include <Core/Settings.h>

namespace DB
{

class QueryPlan;

class Set;
using SetPtr = std::shared_ptr<Set>;
class InterpreterSelectWithUnionQuery;

class IQueryTreeNode;
using QueryTreeNodePtr = std::shared_ptr<IQueryTreeNode>;

struct Settings;

/// Represents a set in a query that might be referenced at analysis time and built later during execution.
/// Also it can represent a constant set that is ready to use.
/// At analysis stage the FutureSets are created but not necessarily filled. Then for non-constant sets there
/// must be an explicit step to build them before they can be used.
/// Set may be useful for indexes, in this case special ordered set with stored elements is build inplace.
class FutureSet
{
public:
    virtual ~FutureSet() = default;

    virtual SetPtr get() const = 0;
    virtual SetPtr buildOrderedSetInplace(const ContextPtr & context) = 0;

    virtual const DataTypes & getTypes() const = 0;
};

using FutureSetPtr = std::shared_ptr<FutureSet>;


class FutureSetFromStorage : public FutureSet
{
public:
    FutureSetFromStorage(SetPtr set_);

    SetPtr get() const override;
    SetPtr buildOrderedSetInplace(const ContextPtr &) override;
    const DataTypes & getTypes() const override;

private:
    SetPtr set;
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
    FutureSetFromSubquery(SubqueryForSet subquery_, FutureSetPtr external_table_set_, const Settings & settings);

    SetPtr get() const override;

    SetPtr buildOrderedSetInplace(const ContextPtr & context) override;

    std::unique_ptr<QueryPlan> build(const ContextPtr & context);

    const DataTypes & getTypes() const override;

    SubqueryForSet & getSubquery() { return subquery; }
    void setQueryPlan(std::unique_ptr<QueryPlan> source);

private:
    SubqueryForSet subquery;
    FutureSetPtr external_table_set;

    std::unique_ptr<QueryPlan> buildPlan(const ContextPtr & context);
};

// struct PreparedSetKey
// {
//     using Hash = std::pair<UInt64, UInt64>;

//     /// Prepared sets for tuple literals are indexed by the hash of the tree contents and by the desired
//     /// data types of set elements (two different Sets can be required for two tuples with the same contents
//     /// if left hand sides of the IN operators have different types).
//     static PreparedSetKey forLiteral(Hash hash, DataTypes types_);

//     /// Prepared sets for subqueries are indexed only by the AST contents because the type of the resulting
//     /// set is fully determined by the subquery.
//     static PreparedSetKey forSubquery(Hash hash);

//     Hash ast_hash;
//     DataTypes types; /// Empty for subqueries.

//     bool operator==(const PreparedSetKey & other) const;

//     String toString() const;

//     struct Hashing
//     {
//         UInt64 operator()(const PreparedSetKey & key) const { return key.ast_hash.first; }
//     };
// };

class PreparedSets
{
public:

    using Hash = std::pair<UInt64, UInt64>;
    struct Hashing
    {
        UInt64 operator()(const Hash & key) const { return key.first ^ key.second; }
    };

    // struct SetAndName
    // {
    //     String name;
    //     std::shared_ptr<FutureSetFromSubquery> set;
    // };

    using SetsFromTuple = std::unordered_map<Hash, std::vector<std::shared_ptr<FutureSet>>, Hashing>;
    using SetsFromStorage = std::unordered_map<Hash, std::shared_ptr<FutureSetFromStorage>, Hashing>;
    using SetsFromSubqueries = std::unordered_map<Hash, std::shared_ptr<FutureSetFromSubquery>, Hashing>;

    FutureSetPtr addFromStorage(const Hash & key, SetPtr set_);
    FutureSetPtr addFromTuple(const Hash & key, Block block, const Settings & settings);
    FutureSetPtr addFromSubquery(const Hash & key, SubqueryForSet subquery, const Settings & settings, FutureSetPtr external_table_set);

    FutureSetPtr findTuple(const Hash & key, const DataTypes & types) const;
    std::shared_ptr<FutureSetFromStorage> findStorage(const Hash & key) const;
    std::shared_ptr<FutureSetFromSubquery> findSubquery(const Hash & key) const;

    //FutureSetPtr getFuture(const PreparedSetKey & key) const;

    /// Get subqueries and clear them.
    /// We need to build a plan for subqueries just once. That's why we can clear them after accessing them.
    /// SetPtr would still be available for consumers of PreparedSets.
    std::vector<std::shared_ptr<FutureSetFromSubquery>> detachSubqueries();

    const SetsFromTuple & getSetsFromTuple() const { return sets_from_tuple; }
    const SetsFromStorage & getSetsFromStorage() const { return sets_from_storage; }
    const SetsFromSubqueries & getSetsFromSubquery() const { return sets_from_subqueries; }

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
