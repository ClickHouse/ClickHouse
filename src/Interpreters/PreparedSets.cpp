#include <chrono>
#include <variant>
#include <Columns/ColumnTuple.h>
#include <Core/Block.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeTuple.h>
#include <IO/Operators.h>
#include <Interpreters/castColumn.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/ProcessorsProfileLog.h>
#include <Interpreters/Set.h>
#include <Interpreters/Context.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Storages/IStorage.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/Sinks/EmptySink.h>
#include <Processors/Sinks/NullSink.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <QueryPipeline/SizeLimits.h>
#include <Common/CurrentThread.h>
#include <Common/FailPoint.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace
{

/// Check if any step in the query plan tree contains correlated expressions (PLACEHOLDER nodes).
/// Such plans cannot be executed standalone — they require decorrelation first.
/// We must traverse both `node->children` and any nested plans returned by `step->getChildPlans()`
/// (e.g. `ReadFromMerge`), otherwise correlated `PLACEHOLDER` actions inside a child plan can be
/// missed and we may attempt standalone execution and hit `Trying to execute PLACEHOLDER action`.
bool hasCorrelatedExpressions(QueryPlan::Node * node)
{
    if (!node)
        return false;

    if (node->step->hasCorrelatedExpressions())
        return true;

    for (auto * child : node->children)
        if (hasCorrelatedExpressions(child))
            return true;

    for (auto * child_plan : node->step->getChildPlans())
        if (child_plan && hasCorrelatedExpressions(child_plan->getRootNode()))
            return true;

    return false;
}

/// Extract a subplan and carry over the parent plan's resources and settings.
/// `QueryPlan::extractSubplan` only moves the node tree; without this the new sub-plan loses
/// `interpreter_context`, table locks, storage holders, and `max_threads`/`concurrency_control`,
/// which keep storage and contexts alive for the delayed build that runs later.
std::unique_ptr<QueryPlan> extractSubplanInheritingResources(QueryPlan & parent_plan, QueryPlan::Node * subplan_root)
{
    auto extracted = std::make_unique<QueryPlan>(parent_plan.extractSubplan(subplan_root));
    extracted->addResources(parent_plan.detachResources());
    extracted->setMaxThreads(parent_plan.getMaxThreads());
    extracted->setConcurrencyControl(parent_plan.getConcurrencyControl());
    return extracted;
}

}

namespace Setting
{
    extern const SettingsUInt64 max_bytes_in_set;
    extern const SettingsUInt64 max_bytes_to_transfer;
    extern const SettingsUInt64 interactive_delay;
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
    extern const int NOT_IMPLEMENTED;
    extern const int SET_SIZE_LIMIT_EXCEEDED;
}

namespace FailPoints
{
    extern const char future_set_from_subquery_skip_inplace_build[];
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


FutureSetFromStorage::FutureSetFromStorage(Hash hash_, ASTPtr ast_, SetPtr set_, std::optional<StorageID> storage_id_)
    : hash(hash_), ast(std::move(ast_)), storage_id(std::move(storage_id_)), set(std::move(set_)) {}
SetPtr FutureSetFromStorage::get() const { return set; }
FutureSet::Hash FutureSetFromStorage::getHash() const { return hash; }
DataTypes FutureSetFromStorage::getTypes() const { return set->getElementsTypes(); }

SetPtr FutureSetFromStorage::buildOrderedSetInplace(const ContextPtr &)
{
    return set->hasExplicitSetElements() ? set : nullptr;
}


FutureSetFromTuple::FutureSetFromTuple(
    Hash hash_, ASTPtr ast_, ColumnsWithTypeAndName block,
    bool transform_null_in, SizeLimits size_limits)
    : hash(hash_), ast(std::move(ast_))
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

void FutureSetFromTuple::fillSetElementsOnce()
{
    callOnce(fill_set_elements_once, [this]
    {
        set->fillSetElements();
        set->appendSetElements(set_key_columns);
    });
}

Columns FutureSetFromTuple::getKeyColumns()
{
    fillSetElementsOnce();
    return set->getSetElements();
}

SetPtr FutureSetFromTuple::buildOrderedSetInplace(const ContextPtr & context)
{
    const auto & settings = context->getSettingsRef();
    size_t max_values = settings[Setting::use_index_for_in_with_subqueries_max_values];
    bool too_many_values = max_values && max_values < set->getTotalRowCount();
    if (!too_many_values)
        fillSetElementsOnce();

    return set;
}


FutureSetFromSubquery::FutureSetFromSubquery(
    Hash hash_,
    ASTPtr ast_,
    std::unique_ptr<QueryPlan> source_,
    QueryPlanBuilder query_plan_builder_,
    StoragePtr external_table,
    std::shared_ptr<FutureSetFromSubquery> external_table_set_,
    bool transform_null_in,
    SizeLimits size_limits,
    size_t max_size_for_index)
    : hash(hash_)
    , ast(std::move(ast_))
    , external_table_set(std::move(external_table_set_))
    , source(std::move(source_))
    , query_plan_builder(std::move(query_plan_builder_))
{
    set_and_key = std::make_shared<SetAndKey>();
    set_and_key->key = PreparedSets::toString(hash_, {});

    set_and_key->set = std::make_shared<Set>(size_limits, max_size_for_index, transform_null_in);
    set_and_key->set->setHeader(source->getCurrentHeader()->getColumnsWithTypeAndName());

    set_and_key->external_table = std::move(external_table);
}

FutureSetFromSubquery::FutureSetFromSubquery(
    Hash hash_,
    ASTPtr ast_,
    QueryTreeNodePtr query_tree_,
    bool transform_null_in,
    SizeLimits size_limits,
    size_t max_size_for_index)
    : hash(hash_), ast(std::move(ast_)), query_tree(std::move(query_tree_))
{
    set_and_key = std::make_shared<SetAndKey>();
    set_and_key->key = PreparedSets::toString(hash_, {});
    set_and_key->set = std::make_shared<Set>(size_limits, max_size_for_index, transform_null_in);
}

FutureSetFromSubquery::~FutureSetFromSubquery() = default;

void FutureSetFromSubquery::replaceSetAndKey(SetAndKeyPtr set)
{
    if (set->key != set_and_key->key)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to exchange sets with different keys: {} vs {}", set->key, set_and_key->key);
    set_and_key = std::move(set);
}

SetAndKeyPtr FutureSetFromSubquery::detachSetAndKey()
{
    SetAndKeyPtr ret;
    std::swap(ret, set_and_key);
    return ret;
}

SetPtr FutureSetFromSubquery::get() const
{
    if (set_and_key->set != nullptr && set_and_key->set->isCreated())
        return set_and_key->set;

    return nullptr;
}

void FutureSetFromSubquery::setQueryPlan(std::unique_ptr<QueryPlan> source_)
{
    source = std::move(source_);
    set_and_key->set->setHeader(source->getCurrentHeader()->getColumnsWithTypeAndName());
}

void FutureSetFromSubquery::setQueryPlanBuilder(QueryPlanBuilder query_plan_builder_)
{
    query_plan_builder = std::move(query_plan_builder_);
}

SetAndKeyPtr FutureSetFromSubquery::createTemporarySetAndKeyForInplaceBuild(bool keep_explicit_set_elements) const
{
    auto result = std::make_shared<SetAndKey>();
    result->key = set_and_key->key;

    size_t max_elements_to_fill = set_and_key->set->max_elements_to_fill;
    if (set_and_key->external_table)
        max_elements_to_fill = 0;

    result->set = std::make_shared<Set>(set_and_key->set->limits, max_elements_to_fill, set_and_key->set->transform_null_in);
    result->set->setHeader(source->getCurrentHeader()->getColumnsWithTypeAndName());

    if (keep_explicit_set_elements || set_and_key->external_table)
        result->set->fillSetElements();

    return result;
}

std::unique_ptr<QueryPlan> FutureSetFromSubquery::createQueryPlanForRetry(const ContextPtr & context) const
{
    if (source)
    {
        try
        {
            return std::make_unique<QueryPlan>(source->clone());
        }
        catch (const Exception & e)
        {
            /// Many `IQueryPlanStep` subclasses do not implement `clone`. If the original plan cannot be
            /// cloned, fall back to rebuilding it via `query_plan_builder`. If neither path is available,
            /// the caller's fallback uses `extractSubplan` on the executed plan instead.
            if (e.code() != ErrorCodes::NOT_IMPLEMENTED)
                throw;
        }
    }

    if (!query_plan_builder)
        return nullptr;

    auto restored_source = query_plan_builder(context);
    if (!restored_source)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Query plan builder returned empty source query plan for set {}",
            set_and_key->key);

    return restored_source;
}

void FutureSetFromSubquery::restoreQueryPlanForRetry(std::unique_ptr<QueryPlan> source_for_retry)
{
    if (!source_for_retry)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot restore empty source query plan for set {} after failed in-place build",
            set_and_key->key);

    setQueryPlan(std::move(source_for_retry));
}

void FutureSetFromSubquery::buildExternalTableFromInplaceSet(StoragePtr external_table_, const SizeLimits & network_transfer_limits)
{
    const auto & set = *set_and_key->set;

    LOG_TRACE(getLogger("FutureSetFromSubquery"), "Building external table from set of {} elements", set.getTotalRowCount());
    if (set.empty())
        return;

    auto metadata = external_table_->getInMemoryMetadataPtr(CurrentThread::tryGetQueryContext(), false);
    const auto & expected_columns = metadata->getColumns().getAllPhysical();

    Columns set_elements = set.getSetElements();
    const auto & set_types = set.getElementsTypes();

    /// The Set class may strip Nullable/LowCardinality from types when storing elements
    /// (depending on transform_null_in setting), so we need to convert the set elements
    /// to match the external table's expected column types.
    Columns converted_columns;
    converted_columns.reserve(set_elements.size());

    size_t idx = 0;
    for (const auto & expected_col : expected_columns)
    {
        if (idx >= set_elements.size())
            break;

        const auto & set_column = set_elements[idx];
        const auto & set_type = set_types[idx];
        const auto & expected_type = expected_col.type;

        if (!set_type->equals(*expected_type))
            converted_columns.push_back(castColumn({set_column, set_type, ""}, expected_type));
        else
            converted_columns.push_back(set_column);

        ++idx;
    }

    Chunk set_chunk(std::move(converted_columns), set.getTotalRowCount());

    const size_t rows_to_transfer = set_chunk.getNumRows();
    const size_t bytes_to_transfer = set_chunk.bytes();

    auto pipeline = QueryPipeline(external_table_->write({}, metadata, nullptr, /*async_insert=*/false));
    PushingPipelineExecutor executor(pipeline);
    executor.push(std::move(set_chunk));
    executor.finish();

    /// Enforce `max_rows_to_transfer` / `max_bytes_to_transfer` on this in-place
    /// external-table write path. The streaming `CreatingSetsTransform` checks
    /// these limits *after* pushing each block, so the block that crosses the
    /// threshold is still transferred and only later blocks are skipped. Here we
    /// write the deduplicated set as a single chunk, so we likewise push first
    /// and check afterwards: with `transfer_overflow_mode = 'throw'` an over-limit
    /// set raises; with `'break'` the chunk has already been written, mirroring
    /// the streaming path that keeps the blocks transferred before the limit was
    /// hit (and avoiding dropping the whole table at the exact boundary, since
    /// `SizeLimits::softCheck` uses `>=`).
    network_transfer_limits.check(
        rows_to_transfer, bytes_to_transfer,
        "IN/JOIN external table",
        ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
}

void FutureSetFromSubquery::setExternalTable(StoragePtr external_table_)
{
    if (set_and_key->set->isCreated())
    {
        if (!set_and_key->set->hasExplicitSetElements())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to attach external table to a ready set without explicit elements");

        /// Attaching an external table to an already-built set preserves prior behavior:
        /// the set elements have already passed in-set size limits, and the network
        /// transfer limits do not apply at attachment time.
        buildExternalTableFromInplaceSet(external_table_, SizeLimits{});
    }

    set_and_key->external_table = std::move(external_table_);
}

DataTypes FutureSetFromSubquery::getTypes() const
{
    return set_and_key->set->getElementsTypes();
}

FutureSet::Hash FutureSetFromSubquery::getHash() const { return hash; }

std::unique_ptr<QueryPlan> FutureSetFromSubquery::build(const SizeLimits & network_transfer_limits, const PreparedSetsCachePtr & prepared_sets_cache)
{
    if (set_and_key->set->isCreated())
        return nullptr;

    auto plan = std::move(source);

    if (!plan)
        return nullptr;

    auto creating_set = std::make_unique<CreatingSetStep>(
        plan->getCurrentHeader(),
        set_and_key,
        network_transfer_limits,
        prepared_sets_cache);
    creating_set->setStepDescription("Create set for subquery");
    plan->addStep(std::move(creating_set));
    return plan;
}

void FutureSetFromSubquery::buildSetInplace(const ContextPtr & context)
{
    if (external_table_set)
        external_table_set->buildSetInplace(context);

    /// Correlated subqueries contain PLACEHOLDER actions that cannot be executed standalone.
    /// They will be decorrelated and executed as part of the outer query instead.
    if (source && hasCorrelatedExpressions(source->getRootNode()))
        return;

    const auto & settings = context->getSettingsRef();
    SizeLimits network_transfer_limits(settings[Setting::max_rows_to_transfer], settings[Setting::max_bytes_to_transfer], settings[Setting::transfer_overflow_mode]);

    if (set_and_key->set->isCreated())
        return;

    if (!source)
        return;

    auto source_for_retry = createQueryPlanForRetry(context);
    auto inplace_set_and_key = createTemporarySetAndKeyForInplaceBuild(false);
    auto plan = std::move(source);
    auto creating_set = std::make_unique<CreatingSetStep>(
        plan->getCurrentHeader(),
        inplace_set_and_key,
        network_transfer_limits,
        /*prepared_sets_cache=*/nullptr);
    creating_set->setStepDescription("Create set for subquery");
    plan->addStep(std::move(creating_set));

    auto builder = plan->buildQueryPipeline(QueryPlanOptimizationSettings(context), BuildQueryPipelineSettings(context));
    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));
    pipeline.complete(std::make_shared<EmptySink>(std::make_shared<const Block>(Block())));

    CompletedPipelineExecutor executor(pipeline);
    if (context->hasQueryContext())
    {
        if (auto cancel_callback = context->getQueryContext()->getInteractiveCancelCallback())
            executor.setCancelCallback(std::move(cancel_callback), std::max(UInt64(100), context->getSettingsRef()[Setting::interactive_delay] / 1000));
    }

    bool skip_inplace_build = false;
    fiu_do_on(FailPoints::future_set_from_subquery_skip_inplace_build, skip_inplace_build = true;);
    if (!skip_inplace_build)
        executor.execute();

    if (!inplace_set_and_key->set->isCreated())
    {
        if (source_for_retry)
        {
            restoreQueryPlanForRetry(std::move(source_for_retry));
        }
        else
        {
            auto * root = plan->getRootNode();
            if (!root || root->step->getName() != "CreatingSet" || root->children.size() != 1)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Cannot restore source query plan for set {} after failed in-place build without retry snapshot",
                    set_and_key->key);

            setQueryPlan(extractSubplanInheritingResources(*plan, root->children.front()));
        }
        return;
    }

    set_and_key->set = std::move(inplace_set_and_key->set);
    if (set_and_key->external_table)
        buildExternalTableFromInplaceSet(set_and_key->external_table, network_transfer_limits);

    /// Finalize write in query cache to save subquery result (no-op if no cache writers exist in the pipeline)
    pipeline.finalizeWriteInQueryResultCache();
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

    /// Correlated subqueries contain PLACEHOLDER actions that cannot be executed standalone.
    /// They will be decorrelated and executed as part of the outer query instead.
    if (source && hasCorrelatedExpressions(source->getRootNode()))
        return nullptr;

    const auto & settings = context->getSettingsRef();
    SizeLimits network_transfer_limits(settings[Setting::max_rows_to_transfer], settings[Setting::max_bytes_to_transfer], settings[Setting::transfer_overflow_mode]);

    if (!source)
        return nullptr;

    auto source_for_retry = createQueryPlanForRetry(context);
    auto inplace_set_and_key = createTemporarySetAndKeyForInplaceBuild(true);
    auto plan = std::move(source);
    auto creating_set = std::make_unique<CreatingSetStep>(
        plan->getCurrentHeader(),
        inplace_set_and_key,
        network_transfer_limits,
        /*prepared_sets_cache=*/nullptr);
    creating_set->setStepDescription("Create set for subquery");
    plan->addStep(std::move(creating_set));

    auto builder = plan->buildQueryPipeline(QueryPlanOptimizationSettings(context), BuildQueryPipelineSettings(context));
    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));
    pipeline.complete(std::make_shared<EmptySink>(std::make_shared<const Block>(Block())));

    CompletedPipelineExecutor executor(pipeline);
    if (context->hasQueryContext())
    {
        if (auto cancel_callback = context->getQueryContext()->getInteractiveCancelCallback())
            executor.setCancelCallback(std::move(cancel_callback), std::max(UInt64(100), context->getSettingsRef()[Setting::interactive_delay] / 1000));
    }

    bool skip_inplace_build = false;
    fiu_do_on(FailPoints::future_set_from_subquery_skip_inplace_build, skip_inplace_build = true;);
    if (!skip_inplace_build)
        executor.execute();

    /// SET may not be created successfully at this step because of the sub-query timeout, but if we have
    /// timeout_overflow_mode set to `break`, no exception is thrown, and the executor just stops executing
    /// the pipeline without setting `set_and_key->set->is_created` to true.
    if (!inplace_set_and_key->set->isCreated())
    {
        if (source_for_retry)
        {
            restoreQueryPlanForRetry(std::move(source_for_retry));
        }
        else
        {
            auto * root = plan->getRootNode();
            if (!root || root->step->getName() != "CreatingSet" || root->children.size() != 1)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Cannot restore source query plan for set {} after failed ordered in-place build without retry snapshot",
                    set_and_key->key);

            setQueryPlan(extractSubplanInheritingResources(*plan, root->children.front()));
        }
        return nullptr;
    }

    set_and_key->set = std::move(inplace_set_and_key->set);
    if (set_and_key->external_table)
        buildExternalTableFromInplaceSet(set_and_key->external_table, network_transfer_limits);
    logProcessorProfile(context, pipeline.getProcessors());

    /// Finalize write in query cache to save subquery result (no-op if no cache writers exist in the pipeline)
    pipeline.finalizeWriteInQueryResultCache();

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

FutureSetFromTuplePtr PreparedSets::addFromTuple(const Hash & key, ASTPtr ast, ColumnsWithTypeAndName block, const Settings & settings)
{
    auto size_limits = getSizeLimitsForSet(settings);
    auto from_tuple = std::make_shared<FutureSetFromTuple>(
        key, std::move(ast), std::move(block),
        settings[Setting::transform_null_in], size_limits);

    const auto & set_types = from_tuple->getTypes();
    auto & sets_by_hash = sets_from_tuple[key];

    for (const auto & set : sets_by_hash)
        if (equals(set->getTypes(), set_types))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Duplicate set: {}", toString(key, set_types));

    sets_by_hash.push_back(from_tuple);
    return from_tuple;
}

FutureSetFromStoragePtr PreparedSets::addFromStorage(const Hash & key, ASTPtr ast, SetPtr set_, StorageID storage_id)
{
    auto from_storage = std::make_shared<FutureSetFromStorage>(key, std::move(ast), std::move(set_), std::move(storage_id));
    auto [it, inserted] = sets_from_storage.emplace(key, from_storage);

    if (!inserted)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Duplicate set: {}", toString(key, {}));

    return from_storage;
}

FutureSetFromSubqueryPtr PreparedSets::addFromSubquery(
    const Hash & key,
    ASTPtr ast,
    std::unique_ptr<QueryPlan> source,
    FutureSetFromSubquery::QueryPlanBuilder query_plan_builder,
    StoragePtr external_table,
    FutureSetFromSubqueryPtr external_table_set,
    const Settings & settings)
{
    auto size_limits = getSizeLimitsForSet(settings);
    auto from_subquery = std::make_shared<FutureSetFromSubquery>(
        key, std::move(ast), std::move(source), std::move(query_plan_builder), std::move(external_table), std::move(external_table_set),
        settings[Setting::transform_null_in], size_limits, settings[Setting::use_index_for_in_with_subqueries_max_values]);

    auto [it, inserted] = sets_from_subqueries.emplace(key, from_subquery);

    if (!inserted)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Duplicate set: {}", toString(key, {}));

    return from_subquery;
}

FutureSetFromSubqueryPtr PreparedSets::addFromSubquery(
    const Hash & key,
    ASTPtr ast,
    QueryTreeNodePtr query_tree,
    const Settings & settings)
{
    auto size_limits = getSizeLimitsForSet(settings);
    auto from_subquery = std::make_shared<FutureSetFromSubquery>(
        key, std::move(ast), std::move(query_tree),
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
