#include <Interpreters/PreparedSets.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/Set.h>

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

SubqueryForSet & PreparedSets::createOrGetSubquery(const String & subquery_id, const PreparedSetKey & key,
                                                   SizeLimits set_size_limit, bool transform_null_in)
{
    SubqueryForSet & subquery = subqueries[subquery_id];

    /// If you already created a Set with the same subquery / table for another ast
    /// In that case several PreparedSetKey would share same subquery and set
    /// Not sure if it's really possible case (maybe for distributed query when set was filled by external table?)
    if (subquery.set)
        sets[key] = subquery.set;
    else
        sets[key] = subquery.set = std::make_shared<Set>(set_size_limit, false, transform_null_in);
    return subquery;
}

/// If the subquery is not associated with any set, create default-constructed SubqueryForSet.
/// It's aimed to fill external table passed to SubqueryForSet::createSource.
SubqueryForSet & PreparedSets::getSubquery(const String & subquery_id) { return subqueries[subquery_id]; }

void PreparedSets::set(const PreparedSetKey & key, SetPtr set_) { sets[key] = set_; }

SetPtr & PreparedSets::get(const PreparedSetKey & key) { return sets[key]; }

std::vector<SetPtr> PreparedSets::getByTreeHash(IAST::Hash ast_hash)
{
    std::vector<SetPtr> res;
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

void SubqueryForSet::createSource(InterpreterSelectWithUnionQuery & interpreter, StoragePtr table_)
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

SetPtr PreparedSetsCache::findOrBuild(const PreparedSetKey & key, const std::function<SetPtr()> & build_set)
{
    auto* log = &Poco::Logger::get("PreparedSetsCache");

    EntryPtr entry;
    bool need_to_build_set = false;

    /// Look for existing entry in the cache.
    {
        std::lock_guard lock(cache_mutex);

        auto it = cache.find(key);
        if (it != cache.end())
        {
            entry = it->second;
        }
        else
        {
            if (build_set == nullptr)
                return nullptr;

            /// Insert the entry into the cache so that other threads can find it and start waiting for the set.
            entry = std::make_shared<Entry>();
            entry->filled_set = entry->promise.get_future();
            cache[key] = entry;
            need_to_build_set = true;
        }
    }

    if (need_to_build_set)
    {
        LOG_DEBUG(log, "Building set for key {}:{}", key.ast_hash.first, key.ast_hash.second);
        try
        {
            auto set = build_set();
            entry->promise.set_value(set);
        }
        catch (...)
        {
            entry->promise.set_exception(std::current_exception());
            throw;
        }

        return entry->filled_set.get();
    }

    if (entry->filled_set.valid() && entry->filled_set.wait_for(std::chrono::seconds(0)) == std::future_status::ready)
    {
        LOG_DEBUG(log, "Found set for key {}:{} without wait", key.ast_hash.first, key.ast_hash.second);
    }
    else
    {
        LOG_DEBUG(log, "Found set for key {}:{} with wait", key.ast_hash.first, key.ast_hash.second);
    }
    return entry->filled_set.get();
}


};
