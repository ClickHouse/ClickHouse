#include <chrono>
#include <variant>
#include <Columns/ColumnTuple.h>
#include <Common/SipHash.h>
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

void FutureSetFromTuple::fillSetElementsOnce() const
{
    callOnce(fill_set_elements_once, [this]
    {
        set->fillSetElements();
        set->appendSetElements(set_key_columns);
    });
}

Columns FutureSetFromTuple::getKeyColumns() const
{
    fillSetElementsOnce();
    return set->getSetElements();
}

size_t FutureSetFromTuple::getInputRowCount() const
{
    /// The deduplication filter built at construction has exactly one entry per input row, so its
    /// size is the number of rows in the original right-hand side (before deduplication), available
    /// in O(1) and without materializing the set elements.
    return set_key_columns.filter ? set_key_columns.filter->size() : 0;
}

FutureSet::Hash FutureSetFromTuple::getContentHash() const
{
    callOnce(content_hash_once, [this] { content_hash = computeContentHash(); });
    return content_hash;
}

Columns FutureSetFromTuple::getUniqueKeyColumns() const
{
    /// Apply the deduplication filter from set_key_columns without calling fillSetElementsOnce().
    /// This avoids permanently materializing set->set_elements, which buildOrderedSetInplace
    /// guards behind use_index_for_in_with_subqueries_max_values. The returned columns are
    /// temporary and freed by the caller.
    const size_t n_cols = set_key_columns.key_columns.size();
    Columns result;
    result.reserve(n_cols);
    if (n_cols > 0 && set_key_columns.filter)
    {
        const auto & filter_data = set_key_columns.filter->getData();
        for (const auto * col : set_key_columns.key_columns)
            result.push_back(col->filter(filter_data, -1));
    }
    return result;
}

FutureSet::Hash FutureSetFromTuple::computeContentHash() const
{
    /// Hash the normalized elements (deduplicated, NULL-filtered, sorted by value) so that
    /// permutations and duplicate inputs produce the same hash. Used by the aggregate
    /// projection matcher (actionsDAGUtils.cpp) to compare IN-clause sets.
    const DataTypes element_types = set->getElementsTypes();
    const Columns normalized = getUniqueKeyColumns();
    const size_t normalized_rows = normalized.empty() ? 0 : normalized[0]->size();

    IColumn::Permutation perm;
    if (!normalized.empty() && normalized_rows > 0)
    {
        EqualRanges ranges{{0, normalized_rows}};
        normalized[0]->getPermutation(
            IColumn::PermutationSortDirection::Ascending,
            IColumn::PermutationSortStability::Stable, 0, 1, perm);
        for (size_t i = 1; i < normalized.size(); ++i)
            normalized[i]->updatePermutation(
                IColumn::PermutationSortDirection::Ascending,
                IColumn::PermutationSortStability::Stable, 0, 1, perm, ranges);
    }

    SipHash siphasher;
    for (size_t i = 0; i < normalized.size(); ++i)
    {
        const auto type_name = element_types[i]->getName();
        siphasher.update(type_name.data(), type_name.size());
        if (!perm.empty())
            normalized[i]->permute(perm, 0)->updateHashFast(siphasher);
        else
            normalized[i]->updateHashFast(siphasher);
    }
    return getSipHash128AsPair(siphasher);
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
    StoragePtr external_table,
    std::shared_ptr<FutureSetFromSubquery> external_table_set_,
    bool transform_null_in,
    SizeLimits size_limits,
    size_t max_size_for_index)
    : hash(hash_), ast(std::move(ast_)), external_table_set(std::move(external_table_set_)), source(std::move(source_))
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

void FutureSetFromSubquery::buildExternalTableFromInplaceSet(StoragePtr external_table_)
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
    auto pipeline = QueryPipeline(external_table_->write({}, metadata, nullptr, /*async_insert=*/false));
    PushingPipelineExecutor executor(pipeline);
    executor.push(std::move(set_chunk));
    executor.finish();
}

void FutureSetFromSubquery::setExternalTable(StoragePtr external_table_)
{
    if (set_and_key->set->isCreated())
    {
        if (!set_and_key->set->hasExplicitSetElements())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to attach external table to a ready set without explicit elements");

        buildExternalTableFromInplaceSet(external_table_);
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
    auto prepared_sets_cache = context->getPreparedSetsCache();

    auto plan = build(network_transfer_limits, prepared_sets_cache);

    if (!plan)
        return;

    auto builder = plan->buildQueryPipeline(QueryPlanOptimizationSettings(context), BuildQueryPipelineSettings(context));
    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));
    pipeline.complete(std::make_shared<EmptySink>(std::make_shared<const Block>(Block())));

    CompletedPipelineExecutor executor(pipeline);
    if (context->hasQueryContext())
    {
        if (auto cancel_callback = context->getQueryContext()->getInteractiveCancelCallback())
            executor.setCancelCallback(std::move(cancel_callback), std::max(UInt64(100), context->getSettingsRef()[Setting::interactive_delay] / 1000));
    }
    executor.execute();

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

    if (!source)
        return nullptr;

    /// Correlated subqueries contain PLACEHOLDER actions that cannot be executed standalone.
    /// They will be decorrelated and executed as part of the outer query instead.
    if (hasCorrelatedExpressions(source->getRootNode()))
        return nullptr;

    const auto & settings = context->getSettingsRef();
    SizeLimits network_transfer_limits(settings[Setting::max_rows_to_transfer], settings[Setting::max_bytes_to_transfer], settings[Setting::transfer_overflow_mode]);

    /// This is a *speculative* build, run during primary key / skip index analysis so that index
    /// analysis can use the set. Prefer a build that does not destroy the canonical `source` plan: if
    /// the in-place pipeline stops without creating the set (e.g. a subquery timeout with
    /// `overflow_mode = 'break'`, or any early stop that skips `CreatingSetsTransform::generate` and
    /// therefore `Set::finishInsert`), the set must still be buildable later by
    /// `DelayedCreatingSetsStep::makePlansForSets`. Otherwise the consumed `source` leaves the set
    /// permanently unbuilt and `FunctionIn` throws "Not-ready Set is passed as the second argument"
    /// when the main pipeline runs.
    ///
    /// So run the pipeline against a clone of `source`, leaving the original intact. Some source steps
    /// cannot be cloned — most notably `ReadFromPreparedSource`, which wraps an already-materialized,
    /// single-use `Pipe` (dictionary, many system table, and remote reads go through it), and
    /// `DelayedCreatingSetsStep`, which a nested `IN` subquery adds to the source plan. The latter is left
    /// non-clonable on purpose: it holds the inner subqueries by shared pointer, and building it consumes
    /// each inner `source` (`DelayedCreatingSetsStep::makePlansForSets` calls `FutureSetFromSubquery::build`,
    /// which moves the inner `source` out). A shallow clone would share those inner subqueries, so a
    /// speculative pass would consume the inner sources and mutate the canonical inner sets anyway — giving
    /// no real preservation. A `MATERIALIZED` CTE referenced by a local `IN` subquery is the same kind of
    /// shape: its source plan contains a `DelayedMaterializingCTEsStep`, which is also non-clonable, so it
    /// takes the destructive fallback too — again identical to the pre-PR behavior for that shape. For any
    /// non-clonable source, fall back to the original destructive build so
    /// primary key analysis is still performed for such subqueries (as it always was); only the rare
    /// silent-failure case stays unrecoverable there, exactly as before this change.
    ///
    /// The non-destructive path is also skipped when there is a `GLOBAL IN` / `GLOBAL JOIN` external
    /// table. Such a build must stream the subquery output into the external table *during* the pipeline
    /// run, both to enforce the network transfer limits (`max_rows_to_transfer` / `max_bytes_to_transfer`,
    /// checked in `CreatingSetsTransform::consume` only while writing the table) and to materialize the
    /// rows actually sent to remote shards — which is not the same as replaying the set elements, because
    /// those may be deduplicated or dropped once `use_index_for_in_with_subqueries_max_values` is exceeded.
    /// Writing to the real external table speculatively would also leave a partial prefix there on a silent
    /// failure that the deferred build would then append to. So for the external-table case use the
    /// destructive build, exactly as before this change; the silent-failure case stays unrecoverable there,
    /// no worse than the previous behavior.
    std::unique_ptr<QueryPlan> plan;
    bool source_preserved = false;
    if (!set_and_key->external_table)
    {
        try
        {
            plan = std::make_unique<QueryPlan>(source->clone());
            source_preserved = true;
        }
        catch (const Exception & e)
        {
            if (e.code() != ErrorCodes::NOT_IMPLEMENTED)
                throw;
        }
    }

    /// On the non-destructive path the speculative pipeline builds into this temporary set; it is
    /// published into `set_and_key` only after the build fully succeeds (see below). It stays null on the
    /// destructive fallback, which builds directly into the canonical `set_and_key->set`.
    SetPtr speculative_set;

    if (source_preserved)
    {
        /// Non-destructive path. Build into a *temporary* `Set` so the canonical `set_and_key->set` is
        /// never mutated unless the build fully succeeds. With `overflow_mode = 'break'` the pipeline can
        /// stop after `CreatingSetsTransform::consume` has inserted a prefix of rows but before
        /// `generate` calls `Set::finishInsert`; building into a temporary set keeps those partial rows
        /// out of the canonical set, which the deferred build reuses. Publishing only a created set means
        /// a non-deterministic subquery (e.g. one using `rand`) is evaluated against a single result.
        ///
        /// Do not share this speculative build through `PreparedSetsCache`: if the in-place pipeline stops
        /// silently, the `CreatingSetsTransform` destructor stores an exception into the cache for this
        /// key, and the later deferred build would rethrow it via `shared_future::get` instead of
        /// rebuilding from the preserved `source`. The deferred build still uses the cache.
        const auto & canonical_set = *set_and_key->set;
        speculative_set = std::make_shared<Set>(canonical_set.limits, canonical_set.max_elements_to_fill, canonical_set.transform_null_in);
        speculative_set->setHeader(plan->getCurrentHeader()->getColumnsWithTypeAndName());
        speculative_set->fillSetElements();

        /// The temporary `SetAndKey` has no external table (the non-destructive path is only taken when
        /// `set_and_key->external_table` is null, see above), so the speculative `CreatingSetsTransform`
        /// only builds the set and never touches a `GLOBAL IN` temporary table.
        auto tmp_set_and_key = std::make_shared<SetAndKey>();
        tmp_set_and_key->key = set_and_key->key;
        tmp_set_and_key->set = speculative_set;

        auto creating_set = std::make_unique<CreatingSetStep>(
            plan->getCurrentHeader(),
            tmp_set_and_key,
            network_transfer_limits,
            /*prepared_sets_cache=*/ nullptr);
        creating_set->setStepDescription("Create set for subquery");
        plan->addStep(std::move(creating_set));
    }
    else
    {
        /// Destructive fallback for non-clonable sources: `build` consumes `source` and binds the
        /// `CreatingSetStep` to the canonical `set_and_key` (as this code always did). On a silent failure
        /// `source` is gone, so the deferred build cannot rebuild — exactly the previous behavior; the set
        /// is never reused with partial rows, because the deferred build throws "Not-ready Set" instead.
        auto prepared_sets_cache = context->getPreparedSetsCache();
        plan = build(network_transfer_limits, prepared_sets_cache);
        if (!plan)
            return nullptr;

        /// `Set::fillSetElements` appends to `set_elements` without clearing, and this function may run
        /// more than once for the same set; fill only once, mirroring `FutureSetFromTuple`.
        if (!set_and_key->set->hasExplicitSetElements())
            set_and_key->set->fillSetElements();
    }

    /// Run the speculative pipeline in its own scope so that `executor`, `pipeline`, and the pipeline
    /// builder are destroyed before `source` is reset below. On the non-destructive path the cloned `plan`
    /// carries an *empty* `QueryPlanResourceHolder` (`QueryPlan::clone` copies the plan nodes only, not the
    /// resources), so the speculative pipeline relies on the original `source` plan to keep the interpreter
    /// contexts, storage holders, and table locks alive: processors may use them implicitly, including in
    /// their destructors — this is exactly what the resource holder normally guarantees for the lifetime of
    /// the pipeline. Resetting `source` while the pipeline is still alive would release them too early. On
    /// the destructive fallback `build` moved the resources into `plan`, which outlives this scope, so the
    /// ordering is safe there too.
    {
        auto builder = plan->buildQueryPipeline(QueryPlanOptimizationSettings(context), BuildQueryPipelineSettings(context));
        auto pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));
        pipeline.complete(std::make_shared<EmptySink>(std::make_shared<const Block>(Block())));

        CompletedPipelineExecutor executor(pipeline);
        if (context->hasQueryContext())
        {
            if (auto cancel_callback = context->getQueryContext()->getInteractiveCancelCallback())
                executor.setCancelCallback(std::move(cancel_callback), std::max(UInt64(100), context->getSettingsRef()[Setting::interactive_delay] / 1000));
        }
        executor.execute();

        /// SET may not be created successfully at this step because of the sub-query timeout, but if we have
        /// `timeout_overflow_mode` set to `break`, no exception is thrown, and the executor just stops executing
        /// the pipeline without setting `is_created` to true. On the non-destructive path the cloned source
        /// above keeps `source` intact, so `DelayedCreatingSetsStep::makePlansForSets` can still build the set
        /// later (on the destructive fallback `source` is already gone, as it always was).
        Set & built_set = speculative_set ? *speculative_set : *set_and_key->set;
        if (!built_set.isCreated())
            return nullptr;

        logProcessorProfile(context, pipeline.getProcessors());

        /// Finalize write in query cache to save subquery result (no-op if no cache writers exist in the pipeline)
        pipeline.finalizeWriteInQueryResultCache();
    }

    /// In-place build succeeded. On the non-destructive path, publish the fully-created temporary set into
    /// the canonical `set_and_key`; the deferred build is then skipped (it checks `isCreated()` / `get()`),
    /// so the original `source` plan is no longer needed. On the destructive fallback `source` was already
    /// consumed by `build`, so `reset` is a no-op there. Reset only now, after the pipeline and executor have
    /// been destroyed, so the resources held by `source` outlive every processor.
    if (speculative_set)
        set_and_key->set = speculative_set;
    source.reset();

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
    StoragePtr external_table,
    FutureSetFromSubqueryPtr external_table_set,
    const Settings & settings)
{
    auto size_limits = getSizeLimitsForSet(settings);
    auto from_subquery = std::make_shared<FutureSetFromSubquery>(
        key, std::move(ast), std::move(source), std::move(external_table), std::move(external_table_set),
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
