#include <chrono>
#include <variant>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/Set.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <IO/Operators.h>
#include <Common/logger_useful.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/Sinks/EmptySink.h>
#include <Processors/Sinks/NullSink.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Core/Block.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <QueryPipeline/SizeLimits.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

class FutureSetFromTuple final : public FutureSet
{
public:
    FutureSetFromTuple(Block block, const Settings & settings);

    SetPtr get() const override { return set; }
    SetPtr buildOrderedSetInplace(const ContextPtr & context) override;

    const DataTypes & getTypes() const override { return set->getElementsTypes(); }

private:
    SetPtr set;
    SetKeyColumns set_key_columns;
};


FutureSetFromStorage::FutureSetFromStorage(SetPtr set_) : set(std::move(set_)) {}
SetPtr FutureSetFromStorage::get() const { return set; }
const DataTypes & FutureSetFromStorage::getTypes() const { return set->getElementsTypes(); }

SetPtr FutureSetFromStorage::buildOrderedSetInplace(const ContextPtr &)
{
    return set->hasExplicitSetElements() ? set : nullptr;
}


// PreparedSetKey PreparedSetKey::forLiteral(Hash hash, DataTypes types_)
// {
//     /// Remove LowCardinality types from type list because Set doesn't support LowCardinality keys now,
//     ///   just converts LowCardinality to ordinary types.
//     for (auto & type : types_)
//         type = recursiveRemoveLowCardinality(type);

//     PreparedSetKey key;
//     key.ast_hash = hash;
//     key.types = std::move(types_);
//     return key;
// }

// PreparedSetKey PreparedSetKey::forSubquery(Hash hash)
// {
//     PreparedSetKey key;
//     key.ast_hash = hash;
//     return key;
// }

// bool PreparedSetKey::operator==(const PreparedSetKey & other) const
// {
//     if (ast_hash != other.ast_hash)
//         return false;

//     if (types.size() != other.types.size())
//         return false;

//     for (size_t i = 0; i < types.size(); ++i)
//     {
//         if (!types[i]->equals(*other.types[i]))
//             return false;
//     }

//     return true;
// }

String PreparedSets::toString(const PreparedSets::Hash & key, const DataTypes & types)
{
    WriteBufferFromOwnString buf;
    buf << "__set_" << key.first << "_" << key.second;
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
// void PreparedSets::addStorageToSubquery(const String & subquery_id, StoragePtr storage)
// {
//     auto it = subqueries.find(subquery_id);
//     if (it == subqueries.end())
//         throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot find subquery {}", subquery_id);

//     it->second->addStorage(std::move(storage));
// }
static bool equals(const DataTypes & lhs, const DataTypes & rhs)
{
    size_t size = lhs.size();
    if (size != rhs.size())
        return false;

    for (size_t i = 0; i < size; ++i)
    {
        if (!lhs[i]->equals(*rhs[i]))
            return false;
    }

    return true;
}

static bool tryInsertSet(std::vector<std::shared_ptr<FutureSet>> & sets, FutureSetPtr new_set)
{
    auto types = new_set->getTypes();
    for (const auto & set : sets)
        if (equals(set->getTypes(), new_set->getTypes()))
            return false;

    sets.push_back(std::move(new_set));
    return true;
}

static FutureSetPtr findSet(const std::vector<std::shared_ptr<FutureSet>> & sets, const DataTypes & types)
{
    for (const auto & set : sets)
        if (equals(set->getTypes(), types))
            return set;

    return nullptr;
}

FutureSetPtr PreparedSets::addFromTuple(const Hash & key, Block block, const Settings & settings)
{
    auto from_tuple = std::make_shared<FutureSetFromTuple>(std::move(block), settings);
    auto & sets_by_hash = sets_from_tuple[key];

    if (!tryInsertSet(sets_by_hash, from_tuple))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Duplicate set: {}", toString(key, from_tuple->getTypes()));

    return from_tuple;
}

FutureSetPtr PreparedSets::addFromStorage(const Hash & key, SetPtr set_)
{
    auto from_storage = std::make_shared<FutureSetFromStorage>(std::move(set_));
    auto [it, inserted] = sets_from_storage.emplace(key, from_storage);

    if (!inserted)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Duplicate set: {}", toString(key, {}));

    return from_storage;
}

FutureSetPtr PreparedSets::addFromSubquery(
    const Hash & key,
    std::unique_ptr<QueryPlan> source,
    StoragePtr external_table,
    FutureSetPtr external_table_set,
    const Settings & settings)
{
    auto from_subquery = std::make_shared<FutureSetFromSubquery>(
        toString(key, {}),
        std::move(source),
        std::move(external_table),
        std::move(external_table_set),
        settings);

    auto [it, inserted] = sets_from_subqueries.emplace(key, from_subquery);

    if (!inserted)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Duplicate set: {}", toString(key, {}));

    return from_subquery;
}

FutureSetPtr PreparedSets::addFromSubquery(
    const Hash & key,
    QueryTreeNodePtr query_tree,
    const Settings & settings)
{
    auto from_subquery = std::make_shared<FutureSetFromSubquery>(
        toString(key, {}),
        std::move(query_tree),
        settings);

    auto [it, inserted] = sets_from_subqueries.emplace(key, from_subquery);

    if (!inserted)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Duplicate set: {}", toString(key, {}));

    return from_subquery;
}

FutureSetPtr PreparedSets::findTuple(const Hash & key, const DataTypes & types) const
{
    auto it = sets_from_tuple.find(key);
    if (it == sets_from_tuple.end())
        return nullptr;

    return findSet(it->second, types);
}

std::shared_ptr<FutureSetFromSubquery> PreparedSets::findSubquery(const Hash & key) const
{
    auto it = sets_from_subqueries.find(key);
    if (it == sets_from_subqueries.end())
        return nullptr;

    return it->second;
}

std::shared_ptr<FutureSetFromStorage> PreparedSets::findStorage(const Hash & key) const
{
    auto it = sets_from_storage.find(key);
    if (it == sets_from_storage.end())
        return nullptr;

    return it->second;
}

// FutureSetPtr PreparedSets::getFuture(const PreparedSetKey & key) const
// {
//     auto it = sets.find(key);
//     if (it == sets.end())
//         return {};
//     return it->second;
// }

// SetPtr PreparedSets::get(const PreparedSetKey & key) const
// {
//     auto it = sets.find(key);
//     if (it == sets.end() || !it->second.isReady())
//         return nullptr;
//     return it->second.get();
// }

// std::vector<FutureSetPtr> PreparedSets::getByTreeHash(IAST::Hash ast_hash) const
// {
//     std::vector<FutureSetPtr> res;
//     for (const auto & it : this->sets)
//     {
//         if (it.first.ast_hash == ast_hash)
//             res.push_back(it.second);
//     }
//     return res;
// }

std::vector<std::shared_ptr<FutureSetFromSubquery>> PreparedSets::getSubqueries()
{
    std::vector<std::shared_ptr<FutureSetFromSubquery>> res;
    res.reserve(sets_from_subqueries.size());
    for (auto & [_, set] : sets_from_subqueries)
        res.push_back(set);

    return res;
}

// void SubqueryForSet::createSource(InterpreterSelectWithUnionQuery & interpreter, StoragePtr table_)
// {
//     source = std::make_unique<QueryPlan>();
//     interpreter.buildQueryPlan(*source);
//     if (table_)
//         table = table_;
// }

// bool SubqueryForSet::hasSource() const
// {
//     return source != nullptr;
// }

// QueryPlanPtr SubqueryForSet::detachSource()
// {
//     auto res = std::move(source);
//     source = nullptr;
//     return res;
// }


std::variant<std::promise<SetPtr>, SharedSet> PreparedSetsCache::findOrPromiseToBuild(const String & key)
{
    std::lock_guard lock(cache_mutex);

    // std::cerr << "PreparedSetsCache::findOrPromiseToBuild " <<  key << "\n" << StackTrace().toString() << std::endl;

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

SetPtr FutureSetFromSubquery::buildOrderedSetInplace(const ContextPtr & context)
{
    if (!context->getSettingsRef().use_index_for_in_with_subqueries)
        return nullptr;

    if (auto set = get())
    {
        if (set->hasExplicitSetElements())
            return set;

        return nullptr;
    }

    if (external_table_set)
    {
        auto set = external_table_set->buildOrderedSetInplace(context);
        if (set)
            return set_and_key->set = set;
    }

    auto plan = buildPlan(context);
    if (!plan)
        return nullptr;

    set_and_key->set->fillSetElements();
    set_and_key->set->initSetElements();
    auto builder = plan->buildQueryPipeline(QueryPlanOptimizationSettings::fromContext(context), BuildQueryPipelineSettings::fromContext(context));
    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));
    pipeline.complete(std::make_shared<EmptySink>(Block()));

    CompletedPipelineExecutor executor(pipeline);
    executor.execute();

    set_and_key->set->checkIsCreated();

    return set_and_key->set;
}

SetPtr FutureSetFromSubquery::get() const
{
    if (set_and_key->set != nullptr && set_and_key->set->isCreated())
        return set_and_key->set;

    return nullptr;
}

std::unique_ptr<QueryPlan> FutureSetFromSubquery::build(const ContextPtr & context)
{
    return buildPlan(context);
}

static SizeLimits getSizeLimitsForSet(const Settings & settings)
{
    return SizeLimits(settings.max_rows_in_set, settings.max_bytes_in_set, settings.set_overflow_mode);
}

std::unique_ptr<QueryPlan> FutureSetFromSubquery::buildPlan(const ContextPtr & context)
{
    if (set_and_key->set->isCreated())
        return nullptr;

    const auto & settings = context->getSettingsRef();

    auto plan = std::move(source);

    if (!plan)
        return nullptr;

    auto creating_set = std::make_unique<CreatingSetStep>(
            plan->getCurrentDataStream(),
            set_and_key,
            external_table,
            SizeLimits(settings.max_rows_to_transfer, settings.max_bytes_to_transfer, settings.transfer_overflow_mode),
            context);
    creating_set->setStepDescription("Create set for subquery");
    plan->addStep(std::move(creating_set));
    return plan;
}

FutureSetFromTuple::FutureSetFromTuple(Block block, const Settings & settings)
{
    bool create_ordered_set = false;
    auto size_limits = getSizeLimitsForSet(settings);
    set = std::make_shared<Set>(size_limits, create_ordered_set, settings.use_index_for_in_with_subqueries_max_values, settings.transform_null_in);
    set->setHeader(block.cloneEmpty().getColumnsWithTypeAndName());

    Columns columns;
    columns.reserve(block.columns());
    for (const auto & column : block)
        columns.emplace_back(column.column);

    set_key_columns.filter = ColumnUInt8::create(block.rows());

    set->insertFromColumns(columns, set_key_columns);
    set->finishInsert();
}

FutureSetFromSubquery::FutureSetFromSubquery(
    String key,
    std::unique_ptr<QueryPlan> source_,
    StoragePtr external_table_,
    FutureSetPtr external_table_set_,
    const Settings & settings)
    : external_table(std::move(external_table_))
    , external_table_set(std::move(external_table_set_))
    , source(std::move(source_))
{
    set_and_key = std::make_shared<SetAndKey>();
    set_and_key->key = std::move(key);

    bool create_ordered_set = false;
    auto size_limits = getSizeLimitsForSet(settings);
    set_and_key->set = std::make_shared<Set>(size_limits, create_ordered_set, settings.use_index_for_in_with_subqueries_max_values, settings.transform_null_in);
    set_and_key->set->setHeader(source->getCurrentDataStream().header.getColumnsWithTypeAndName());
}

FutureSetFromSubquery::FutureSetFromSubquery(
    String key,
    QueryTreeNodePtr query_tree_,
    //FutureSetPtr external_table_set_,
    const Settings & settings)
    : query_tree(std::move(query_tree_))
{
    set_and_key = std::make_shared<SetAndKey>();
    set_and_key->key = std::move(key);

    bool create_ordered_set = false;
    auto size_limits = getSizeLimitsForSet(settings);
    set_and_key->set = std::make_shared<Set>(size_limits, create_ordered_set, settings.use_index_for_in_with_subqueries_max_values, settings.transform_null_in);
}

void FutureSetFromSubquery::setQueryPlan(std::unique_ptr<QueryPlan> source_)
{
    source = std::move(source_);
    set_and_key->set->setHeader(source->getCurrentDataStream().header.getColumnsWithTypeAndName());
}

const DataTypes & FutureSetFromSubquery::getTypes() const
{
    return set_and_key->set->getElementsTypes();
}

SetPtr FutureSetFromTuple::buildOrderedSetInplace(const ContextPtr & context)
{
    if (set->hasExplicitSetElements())
        return set;

    const auto & settings = context->getSettingsRef();
    size_t max_values = settings.use_index_for_in_with_subqueries_max_values;
    bool too_many_values = max_values && max_values < set->getTotalRowCount();
    if (!too_many_values)
    {
        set->initSetElements();
        set->appendSetElements(set_key_columns);
    }

    return set;
}

};
