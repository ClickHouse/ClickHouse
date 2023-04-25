#include <chrono>
#include <variant>
#include <Interpreters/PreparedSets.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/Set.h>
#include <IO/Operators.h>
#include "Common/logger_useful.h"
#include "Processors/QueryPlan/CreatingSetsStep.h"

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

// SubqueryForSet & PreparedSets::createOrGetSubquery(const String & subquery_id, const PreparedSetKey & key,
//                                                    SizeLimits set_size_limit, bool transform_null_in)
// {
//     SubqueryForSet & subquery = subqueries[subquery_id];

//     /// If you already created a Set with the same subquery / table for another ast
//     /// In that case several PreparedSetKey would share same subquery and set
//     /// Not sure if it's really possible case (maybe for distributed query when set was filled by external table?)
//     if (subquery.set.isValid())
//         sets[key] = subquery.set;
//     else
//     {
//         subquery.set_in_progress = std::make_shared<Set>(set_size_limit, false, transform_null_in);
//         sets[key] = FutureSet(subquery.promise_to_fill_set.get_future());
//     }

//     if (!subquery.set_in_progress)
//     {
//         subquery.key = key.toString();
//         subquery.set_in_progress = std::make_shared<Set>(set_size_limit, false, transform_null_in);
//     }

//     return subquery;
// }

/// If the subquery is not associated with any set, create default-constructed SubqueryForSet.
/// It's aimed to fill external table passed to SubqueryForSet::createSource.
void PreparedSets::addStorageToSubquery(const String & subquery_id, StoragePtr storage)
{
    auto it = subqueries.find(subquery_id);
    if (it == subqueries.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot find subquery {}", subquery_id);

    it->second->addStorage(std::move(storage));
}

FutureSetPtr PreparedSets::addFromStorage(const PreparedSetKey & key, SetPtr set_)
{
    auto from_storage = std::make_shared<FutureSetFromStorage>(std::move(set_));
    auto [it, inserted] = sets.emplace(key, std::move(from_storage));

    if (!inserted)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Duplicate set: {}", key.toString());

    return it->second;
}

FutureSetPtr PreparedSets::addFromTuple(const PreparedSetKey & key, Block block)
{
    auto from_tuple = std::make_shared<FutureSetFromTuple>(std::move(block));
    auto [it, inserted] = sets.emplace(key, std::move(from_tuple));

    if (!inserted)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Duplicate set: {}", key.toString());

    return it->second;
}

FutureSetPtr PreparedSets::addFromSubquery(const PreparedSetKey & key, SubqueryForSet subquery)
{
    auto id = subquery.key;
    auto from_subquery = std::make_shared<FutureSetFromSubquery>(std::move(subquery));
    auto [it, inserted] = sets.emplace(key, from_subquery);

    if (!inserted)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Duplicate set: {}", key.toString());

    subqueries.emplace(id, std::move(from_subquery));
    return it->second;
}

FutureSetPtr PreparedSets::getFuture(const PreparedSetKey & key) const
{
    auto it = sets.find(key);
    if (it == sets.end())
        return {};
    return it->second;
}

// SetPtr PreparedSets::get(const PreparedSetKey & key) const
// {
//     auto it = sets.find(key);
//     if (it == sets.end() || !it->second.isReady())
//         return nullptr;
//     return it->second.get();
// }

// std::vector<FutureSet> PreparedSets::getByTreeHash(IAST::Hash ast_hash) const
// {
//     std::vector<FutureSet> res;
//     for (const auto & it : this->sets)
//     {
//         if (it.first.ast_hash == ast_hash)
//             res.push_back(it.second);
//     }
//     return res;
// }

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

std::unique_ptr<QueryPlan> FutureSetFromSubquery::buildPlan(const ContextPtr & context, bool create_ordered_set)
{
    if (set)
        return nullptr;

    auto set_cache = context->getPreparedSetsCache();
    if (set_cache)
    {
        auto from_cache = set_cache->findOrPromiseToBuild(subquery.key);
        if (from_cache.index() == 0)
        {
            subquery.promise_to_fill_set = std::move(std::get<0>(from_cache));
        }
        else
        {
            LOG_TRACE(&Poco::Logger::get("FutureSetFromSubquery"), "Waiting for set, key: {}", subquery.key);
            set = std::get<1>(from_cache).get();
            return nullptr;
        }
    }


    const auto & settings = context->getSettingsRef();
    auto size_limits = getSizeLimitsForSet(settings, create_ordered_set);

    subquery.set = set = std::make_shared<Set>(size_limits, create_ordered_set, settings.transform_null_in);

    auto plan = subquery.detachSource();
    auto description = subquery.key;

    auto creating_set = std::make_unique<CreatingSetStep>(
            plan->getCurrentDataStream(),
            description,
            std::move(subquery),
            SizeLimits(settings.max_rows_to_transfer, settings.max_bytes_to_transfer, settings.transfer_overflow_mode),
            context);
    creating_set->setStepDescription("Create set for subquery");
    plan->addStep(std::move(creating_set));
    return plan;
}


static SizeLimits getSizeLimitsForUnorderedSet(const Settings & settings)
{
    return SizeLimits(settings.max_rows_in_set, settings.max_bytes_in_set, settings.set_overflow_mode);
}

static SizeLimits getSizeLimitsForOrderedSet(const Settings & settings)
{
    if (settings.use_index_for_in_with_subqueries_max_values &&
        settings.use_index_for_in_with_subqueries_max_values < settings.max_rows_in_set)
        return getSizeLimitsForUnorderedSet(settings);

    return SizeLimits(settings.use_index_for_in_with_subqueries_max_values, settings.max_bytes_in_set, OverflowMode::BREAK);
}

SizeLimits FutureSet::getSizeLimitsForSet(const Settings & settings, bool ordered_set)
{
    return ordered_set ? getSizeLimitsForOrderedSet(settings) : getSizeLimitsForUnorderedSet(settings);
}

};
