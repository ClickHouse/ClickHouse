#include <chrono>
#include <variant>
#include <Interpreters/PreparedSets.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/Set.h>
#include <IO/Operators.h>

namespace DB
{

PreparedSetKey PreparedSetKey::forLiteral(const IAST & ast, DataTypes types_)
{
    /// Remove LowCardinality types from type list because Set doesn't support LowCardinality keys now,
    ///   just converts LowCardinality to ordinary types.
    for (auto & type : types_)
        type = recursiveRemoveLowCardinality(type);

    PreparedSetKey key;
    key.ast_hash = ast.getTreeHash();
    key.types = std::move(types_);
    return key;
}

PreparedSetKey PreparedSetKey::forSubquery(const IAST & ast)
{
    PreparedSetKey key;
    key.ast_hash = ast.getTreeHash();
    return key;
}

bool PreparedSetKey::operator==(const PreparedSetKey & other) const
{
    if (ast_hash != other.ast_hash)
        return false;

    if (types.size() != other.types.size())
        return false;

    for (size_t i = 0; i < types.size(); ++i)
    {
        if (!types[i]->equals(*other.types[i]))
            return false;
    }

    return true;
}

String PreparedSetKey::toString() const
{
    WriteBufferFromOwnString buf;
    buf << "__set_" << ast_hash.first << "_" << ast_hash.second;
    if (!types.empty())
    {
        buf << "(";
        bool first = true;
        for (const auto & type : types)
        {
            if (!first)
                buf << ",";
            first = false;
            buf << type->getName();
        }
        buf << ")";
    }
    return buf.str();
}

SubqueryForSet & PreparedSets::createOrGetSubquery(const String & subquery_id, const PreparedSetKey & key,
                                                   SizeLimits set_size_limit, bool transform_null_in)
{
    SubqueryForSet & subquery = subqueries[subquery_id];

    /// If you already created a Set with the same subquery / table for another ast
    /// In that case several PreparedSetKey would share same subquery and set
    /// Not sure if it's really possible case (maybe for distributed query when set was filled by external table?)
    if (subquery.set.isValid())
        sets[key] = subquery.set;
    else
    {
        subquery.set_in_progress = std::make_shared<Set>(set_size_limit, false, transform_null_in);
        sets[key] = FutureSet(subquery.promise_to_fill_set.get_future());
    }

    if (!subquery.set_in_progress)
    {
        subquery.key = key.toString();
        subquery.set_in_progress = std::make_shared<Set>(set_size_limit, false, transform_null_in);
    }

    return subquery;
}

/// If the subquery is not associated with any set, create default-constructed SubqueryForSet.
/// It's aimed to fill external table passed to SubqueryForSet::createSource.
SubqueryForSet & PreparedSets::getSubquery(const String & subquery_id) { return subqueries[subquery_id]; }

void PreparedSets::set(const PreparedSetKey & key, SetPtr set_) { sets[key] = FutureSet(set_); }

FutureSet PreparedSets::getFuture(const PreparedSetKey & key) const
{
    auto it = sets.find(key);
    if (it == sets.end())
        return {};
    return it->second;
}

SetPtr PreparedSets::get(const PreparedSetKey & key) const
{
    auto it = sets.find(key);
    if (it == sets.end() || !it->second.isReady())
        return nullptr;
    return it->second.get();
}

std::vector<FutureSet> PreparedSets::getByTreeHash(IAST::Hash ast_hash) const
{
    std::vector<FutureSet> res;
    for (const auto & it : this->sets)
    {
        if (it.first.ast_hash == ast_hash)
            res.push_back(it.second);
    }
    return res;
}

PreparedSets::SubqueriesForSets PreparedSets::detachSubqueries()
{
    auto res = std::move(subqueries);
    subqueries = SubqueriesForSets();
    return res;
}

bool PreparedSets::empty() const { return sets.empty(); }

void SubqueryForSet::createSource(IInterpreterUnionOrSelectQuery & interpreter, StoragePtr table_)
{
    source = std::make_unique<QueryPlan>();
    interpreter.buildQueryPlan(*source);
    if (table_)
        table = table_;
}

bool SubqueryForSet::hasSource() const
{
    return source != nullptr;
}

QueryPlanPtr SubqueryForSet::detachSource()
{
    auto res = std::move(source);
    source = nullptr;
    return res;
}


FutureSet::FutureSet(SetPtr set)
{
    std::promise<SetPtr> promise;
    promise.set_value(set);
    *this = FutureSet(promise.get_future());
}


bool FutureSet::isReady() const
{
    return future_set.valid() &&
        future_set.wait_for(std::chrono::seconds(0)) == std::future_status::ready;
}

bool FutureSet::isCreated() const
{
    return isReady() && get() != nullptr && get()->isCreated();
}


std::variant<std::promise<SetPtr>, SharedSet> PreparedSetsCache::findOrPromiseToBuild(const String & key)
{
    std::lock_guard lock(cache_mutex);

    auto it = cache.find(key);
    if (it != cache.end())
    {
        /// If the set is being built, return its future, but if it's ready and is nullptr then we should retry building it.
        if (it->second.future.valid() &&
            (it->second.future.wait_for(std::chrono::seconds(0)) != std::future_status::ready || it->second.future.get() != nullptr))
            return it->second.future;
    }

    /// Insert the entry into the cache so that other threads can find it and start waiting for the set.
    std::promise<SetPtr> promise_to_fill_set;
    Entry & entry = cache[key];
    entry.future = promise_to_fill_set.get_future();
    return promise_to_fill_set;
}

};
