#include <chrono>
#include <variant>
#include <Core/Block.h>
#include <Core/Settings.h>
#include <IO/Operators.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/ProcessorsProfileLog.h>
#include <Interpreters/Set.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/Sinks/EmptySink.h>
#include <Processors/Sinks/NullSink.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <QueryPipeline/SizeLimits.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 max_bytes_in_set;
    extern const SettingsUInt64 max_bytes_to_transfer;
    extern const SettingsUInt64 max_rows_in_set;
    extern const SettingsUInt64 max_rows_to_transfer;
    extern const SettingsOverflowMode set_overflow_mode;
    extern const SettingsOverflowMode transfer_overflow_mode;
    extern const SettingsBool transform_null_in;
    extern const SettingsBool use_index_for_in_with_subqueries;
    extern const SettingsUInt64 use_index_for_in_with_subqueries_max_values;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

SizeLimits PreparedSets::getSizeLimitsForSet(const Settings & settings)
{
    return SizeLimits(settings[Setting::max_rows_in_set], settings[Setting::max_bytes_in_set], settings[Setting::set_overflow_mode]);
}

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


FutureSetFromStorage::FutureSetFromStorage(Hash hash_, SetPtr set_) : hash(hash_), set(std::move(set_)) {}
SetPtr FutureSetFromStorage::get() const { return set; }
FutureSet::Hash FutureSetFromStorage::getHash() const { return hash; }
DataTypes FutureSetFromStorage::getTypes() const { return set->getElementsTypes(); }

SetPtr FutureSetFromStorage::buildOrderedSetInplace(const ContextPtr &)
{
    return set->hasExplicitSetElements() ? set : nullptr;
}


FutureSetFromTuple::FutureSetFromTuple(
    Hash hash_, ColumnsWithTypeAndName block,
    bool transform_null_in, SizeLimits size_limits)
    : hash(hash_)
{
    ColumnsWithTypeAndName header = block;
    for (auto & elem : header)
        elem.column = elem.column->cloneEmpty();

    set = std::make_shared<Set>(size_limits, 0, transform_null_in);

    set->setHeader(header);
    Columns columns;
    columns.reserve(block.size());
    size_t num_rows = 0;
    for (const auto & column : block)
    {
        columns.emplace_back(column.column);
        num_rows = column.column->size();
    }

    set_key_columns.filter = ColumnUInt8::create(num_rows);

    set->insertFromColumns(columns, set_key_columns);
    set->finishInsert();

    for (const auto & type : set->getElementsTypes())
    {
        auto name = type->getName();
        hash = CityHash_v1_0_2::CityHash128WithSeed(name.data(), name.size(), hash);
    }
}

DataTypes FutureSetFromTuple::getTypes() const { return set->getElementsTypes(); }
FutureSet::Hash FutureSetFromTuple::getHash() const { return hash; }

SetPtr FutureSetFromTuple::buildOrderedSetInplace(const ContextPtr & context)
{
    if (set->hasExplicitSetElements())
        return set;

    const auto & settings = context->getSettingsRef();
    size_t max_values = settings[Setting::use_index_for_in_with_subqueries_max_values];
    bool too_many_values = max_values && max_values < set->getTotalRowCount();
    if (!too_many_values)
    {
        set->fillSetElements();
        set->appendSetElements(set_key_columns);
    }

    return set;
}


FutureSetFromSubquery::FutureSetFromSubquery(
    Hash hash_,
    std::unique_ptr<QueryPlan> source_,
    StoragePtr external_table_,
    std::shared_ptr<FutureSetFromSubquery> external_table_set_,
    bool transform_null_in,
    SizeLimits size_limits,
    size_t max_size_for_index)
    : hash(hash_), external_table(std::move(external_table_)), external_table_set(std::move(external_table_set_)), source(std::move(source_))
{
    set_and_key = std::make_shared<SetAndKey>();
    set_and_key->key = PreparedSets::toString(hash_, {});

    set_and_key->set = std::make_shared<Set>(size_limits, max_size_for_index, transform_null_in);
    set_and_key->set->setHeader(source->getCurrentHeader().getColumnsWithTypeAndName());
}

FutureSetFromSubquery::FutureSetFromSubquery(
    Hash hash_,
    QueryTreeNodePtr query_tree_,
    bool transform_null_in,
    SizeLimits size_limits,
    size_t max_size_for_index)
    : hash(hash_), query_tree(std::move(query_tree_))
{
    set_and_key = std::make_shared<SetAndKey>();
    set_and_key->key = PreparedSets::toString(hash_, {});
    set_and_key->set = std::make_shared<Set>(size_limits, max_size_for_index, transform_null_in);
}

FutureSetFromSubquery::~FutureSetFromSubquery() = default;

SetPtr FutureSetFromSubquery::get() const
{
    if (set_and_key->set != nullptr && set_and_key->set->isCreated())
        return set_and_key->set;

    return nullptr;
}

void FutureSetFromSubquery::setQueryPlan(std::unique_ptr<QueryPlan> source_)
{
    source = std::move(source_);
    set_and_key->set->setHeader(source->getCurrentHeader().getColumnsWithTypeAndName());
}

DataTypes FutureSetFromSubquery::getTypes() const
{
    return set_and_key->set->getElementsTypes();
}

FutureSet::Hash FutureSetFromSubquery::getHash() const { return hash; }

std::unique_ptr<QueryPlan> FutureSetFromSubquery::build(const ContextPtr & context)
{
    if (set_and_key->set->isCreated())
        return nullptr;

    const auto & settings = context->getSettingsRef();

    auto plan = std::move(source);

    if (!plan)
        return nullptr;

    auto creating_set = std::make_unique<CreatingSetStep>(
        plan->getCurrentHeader(),
        set_and_key,
        external_table,
        SizeLimits(settings[Setting::max_rows_to_transfer], settings[Setting::max_bytes_to_transfer], settings[Setting::transfer_overflow_mode]),
        context);
    creating_set->setStepDescription("Create set for subquery");
    plan->addStep(std::move(creating_set));
    return plan;
}

void FutureSetFromSubquery::buildSetInplace(const ContextPtr & context)
{
    if (external_table_set)
        external_table_set->buildSetInplace(context);

    auto plan = build(context);

    if (!plan)
        return;

    auto builder = plan->buildQueryPipeline(QueryPlanOptimizationSettings::fromContext(context), BuildQueryPipelineSettings::fromContext(context));
    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));
    pipeline.complete(std::make_shared<EmptySink>(Block()));

    CompletedPipelineExecutor executor(pipeline);
    executor.execute();
}

SetPtr FutureSetFromSubquery::buildOrderedSetInplace(const ContextPtr & context)
{
    if (!context->getSettingsRef()[Setting::use_index_for_in_with_subqueries])
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
        {
            set_and_key->set = set;
            return set_and_key->set;
        }
    }

    auto plan = build(context);
    if (!plan)
        return nullptr;

    set_and_key->set->fillSetElements();
    auto builder = plan->buildQueryPipeline(QueryPlanOptimizationSettings::fromContext(context), BuildQueryPipelineSettings::fromContext(context));
    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));
    pipeline.complete(std::make_shared<EmptySink>(Block()));

    CompletedPipelineExecutor executor(pipeline);
    executor.execute();

    /// SET may not be created successfully at this step because of the sub-query timeout, but if we have
    /// timeout_overflow_mode set to `break`, no exception is thrown, and the executor just stops executing
    /// the pipeline without setting `set_and_key->set->is_created` to true.
    if (!set_and_key->set->isCreated())
        return nullptr;

    logProcessorProfile(context, pipeline.getProcessors());

    return set_and_key->set;
}


String PreparedSets::toString(const PreparedSets::Hash & key, const DataTypes & types)
{
    WriteBufferFromOwnString buf;
    buf << "__set_" << DB::toString(key);
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

FutureSetFromTuplePtr PreparedSets::addFromTuple(const Hash & key, ColumnsWithTypeAndName block, const Settings & settings)
{
    auto size_limits = getSizeLimitsForSet(settings);
    auto from_tuple = std::make_shared<FutureSetFromTuple>(
        key, std::move(block),
        settings[Setting::transform_null_in], size_limits);
    const auto & set_types = from_tuple->getTypes();
    auto & sets_by_hash = sets_from_tuple[key];

    for (const auto & set : sets_by_hash)
        if (equals(set->getTypes(), set_types))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Duplicate set: {}", toString(key, set_types));

    sets_by_hash.push_back(from_tuple);
    return from_tuple;
}

FutureSetFromStoragePtr PreparedSets::addFromStorage(const Hash & key, SetPtr set_)
{
    auto from_storage = std::make_shared<FutureSetFromStorage>(key, std::move(set_));
    auto [it, inserted] = sets_from_storage.emplace(key, from_storage);

    if (!inserted)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Duplicate set: {}", toString(key, {}));

    return from_storage;
}

FutureSetFromSubqueryPtr PreparedSets::addFromSubquery(
    const Hash & key,
    std::unique_ptr<QueryPlan> source,
    StoragePtr external_table,
    FutureSetFromSubqueryPtr external_table_set,
    const Settings & settings)
{
    auto size_limits = getSizeLimitsForSet(settings);
    auto from_subquery = std::make_shared<FutureSetFromSubquery>(
        key, std::move(source), std::move(external_table), std::move(external_table_set),
        settings[Setting::transform_null_in], size_limits, settings[Setting::use_index_for_in_with_subqueries_max_values]);

    auto [it, inserted] = sets_from_subqueries.emplace(key, from_subquery);

    if (!inserted)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Duplicate set: {}", toString(key, {}));

    return from_subquery;
}

FutureSetFromSubqueryPtr PreparedSets::addFromSubquery(
    const Hash & key,
    QueryTreeNodePtr query_tree,
    const Settings & settings)
{
    auto size_limits = getSizeLimitsForSet(settings);
    auto from_subquery = std::make_shared<FutureSetFromSubquery>(
        key, std::move(query_tree),
        settings[Setting::transform_null_in], size_limits, settings[Setting::use_index_for_in_with_subqueries_max_values]);

    auto [it, inserted] = sets_from_subqueries.emplace(key, from_subquery);

    if (!inserted)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Duplicate set: {}", toString(key, {}));

    return from_subquery;
}

FutureSetFromTuplePtr PreparedSets::findTuple(const Hash & key, const DataTypes & types) const
{
    auto it = sets_from_tuple.find(key);
    if (it == sets_from_tuple.end())
        return nullptr;

    for (const auto & set : it->second)
        if (equals(set->getTypes(), types))
            return set;

    return nullptr;
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

PreparedSets::Subqueries PreparedSets::getSubqueries() const
{
    PreparedSets::Subqueries res;
    res.reserve(sets_from_subqueries.size());
    for (const auto & [_, set] : sets_from_subqueries)
        res.push_back(set);

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

};
