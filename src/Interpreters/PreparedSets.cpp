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

    bool isReady() const override { return true; }
    SetPtr get() const override { return set; }

    SetPtr buildOrderedSetInplace(const ContextPtr & context) override;

    DataTypes getTypes() const override { return set->getElementsTypes(); }

private:
    SetPtr set;
    SetKeyColumns set_key_columns;
};


FutureSetFromStorage::FutureSetFromStorage(SetPtr set_) : set(std::move(set_)) {}
bool FutureSetFromStorage::isReady() const { return set != nullptr; }
SetPtr FutureSetFromStorage::get() const { return set; }
DataTypes FutureSetFromStorage::getTypes() const { return set->getElementsTypes(); }

SetPtr FutureSetFromStorage::buildOrderedSetInplace(const ContextPtr &)
{
    return set->hasExplicitSetElements() ? set : nullptr;
}


PreparedSetKey PreparedSetKey::forLiteral(Hash hash, DataTypes types_)
{
    /// Remove LowCardinality types from type list because Set doesn't support LowCardinality keys now,
    ///   just converts LowCardinality to ordinary types.
    for (auto & type : types_)
        type = recursiveRemoveLowCardinality(type);

    PreparedSetKey key;
    key.ast_hash = hash;
    key.types = std::move(types_);
    return key;
}

PreparedSetKey PreparedSetKey::forSubquery(Hash hash)
{
    PreparedSetKey key;
    key.ast_hash = hash;
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
// void PreparedSets::addStorageToSubquery(const String & subquery_id, StoragePtr storage)
// {
//     auto it = subqueries.find(subquery_id);
//     if (it == subqueries.end())
//         throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot find subquery {}", subquery_id);

//     it->second->addStorage(std::move(storage));
// }

FutureSetPtr PreparedSets::addFromStorage(const PreparedSetKey & key, SetPtr set_)
{
    auto from_storage = std::make_shared<FutureSetFromStorage>(std::move(set_));
    auto [it, inserted] = sets.emplace(key, std::move(from_storage));

    if (!inserted)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Duplicate set: {}", key.toString());

    return it->second;
}

FutureSetPtr PreparedSets::addFromTuple(const PreparedSetKey & key, Block block, const Settings & settings)
{
    auto from_tuple = std::make_shared<FutureSetFromTuple>(std::move(block), settings);
    auto [it, inserted] = sets.emplace(key, std::move(from_tuple));

    if (!inserted)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Duplicate set: {}", key.toString());

    return it->second;
}

FutureSetPtr PreparedSets::addFromSubquery(const PreparedSetKey & key, SubqueryForSet subquery, const Settings & settings, FutureSetPtr external_table_set)
{
    auto id = subquery.key;
    auto from_subquery = std::make_shared<FutureSetFromSubquery>(std::move(subquery), std::move(external_table_set), settings.transform_null_in);
    auto [it, inserted] = sets.emplace(key, from_subquery);

    if (!inserted)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Duplicate set: {}", key.toString());

    // std::cerr << key.toString() << std::endl;
    // std::cerr << "========= PreparedSets::addFromSubquery\n";
    // std::cerr << StackTrace().toString() << std::endl;

    subqueries.emplace_back(SetAndName{.name = id, .set = std::move(from_subquery)});
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

    if (subquery.set)
    {
        if (subquery.set->hasExplicitSetElements())
            return subquery.set;

        return nullptr;
    }

    if (external_table_set)
        return subquery.set = external_table_set->buildOrderedSetInplace(context);

    auto plan = buildPlan(context, true);
    if (!plan)
        return nullptr;

    auto builder = plan->buildQueryPipeline(QueryPlanOptimizationSettings::fromContext(context), BuildQueryPipelineSettings::fromContext(context));
    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));
    pipeline.complete(std::make_shared<EmptySink>(Block()));

    CompletedPipelineExecutor executor(pipeline);
    executor.execute();

    subquery.set->checkIsCreated();

    return subquery.set;
}

bool FutureSetFromSubquery::isReady() const { return subquery.set != nullptr && subquery.set->isCreated(); }

std::unique_ptr<QueryPlan> FutureSetFromSubquery::build(const ContextPtr & context)
{
    return buildPlan(context, false);
}

static SizeLimits getSizeLimitsForSet(const Settings & settings)
{
    return SizeLimits(settings.max_rows_in_set, settings.max_bytes_in_set, settings.set_overflow_mode);
}

std::unique_ptr<QueryPlan> FutureSetFromSubquery::buildPlan(const ContextPtr & context, bool create_ordered_set)
{
    if (subquery.set)
        return nullptr;

    const auto & settings = context->getSettingsRef();
    auto size_limits = getSizeLimitsForSet(settings);

    subquery.set = std::make_shared<Set>(size_limits, create_ordered_set, settings.use_index_for_in_with_subqueries_max_values, settings.transform_null_in);

    auto plan = subquery.detachSource();
    auto description = subquery.key;

    auto creating_set = std::make_unique<CreatingSetStep>(
            plan->getCurrentDataStream(),
            description,
            subquery,
            shared_from_this(),
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

FutureSetFromSubquery::FutureSetFromSubquery(SubqueryForSet subquery_, FutureSetPtr external_table_set_, bool transform_null_in_)
    : subquery(std::move(subquery_)), external_table_set(std::move(external_table_set_)), transform_null_in(transform_null_in_) {}

DataTypes FutureSetFromSubquery::getTypes() const
{
    if (subquery.set)
        return subquery.set->getElementsTypes();

    return Set::getElementTypes(subquery.source->getCurrentDataStream().header.getColumnsWithTypeAndName(), transform_null_in);
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
