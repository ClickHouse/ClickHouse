#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/MaterializingCTEStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Transforms/CreatingSetsTransform.h>
#include <Processors/Transforms/DistinctTransform.h>
#include <IO/Operators.h>
#include <Common/JSONBuilder.h>
#include <Core/Settings.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/Set.h>
#include <Interpreters/Context.h>
#include <Processors/QueryPlan/ReadFromLocalReplica.h>
#include <Common/typeid_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace Setting
{
    extern const SettingsUInt64 max_bytes_to_transfer;
    extern const SettingsUInt64 max_rows_to_transfer;
    extern const SettingsOverflowMode transfer_overflow_mode;
}

static ITransformingStep::Traits getTraits()
{
    return ITransformingStep::Traits
    {
        {
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = true,
        },
        {
            .preserves_number_of_rows = true,
        }
    };
}

CreatingSetStep::CreatingSetStep(
    const SharedHeader & input_header_,
    SetAndKeyPtr set_and_key_,
    SizeLimits network_transfer_limits_,
    PreparedSetsCachePtr prepared_sets_cache_)
    : ITransformingStep(input_header_, std::make_shared<const Block>(Block{}), getTraits())
    , set_and_key(std::move(set_and_key_))
    , network_transfer_limits(std::move(network_transfer_limits_))
    , prepared_sets_cache(std::move(prepared_sets_cache_))
{
}

void CreatingSetStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    /// With a single input stream the set fill deduplicates just as well on its own; the pre-distinct
    /// only pays off by deduplicating disjoint streams in parallel. The partition count can drop to one
    /// after the flag was set (e.g. a later filter pushdown re-runs part selection), so check the final
    /// stream count here. The external table is re-checked too: `GLOBAL IN` under the analyzer attaches
    /// it only at pipeline build time (see `ReadFromRemote`), after the optimization passes checked it,
    /// and pre-deduplication would change the table contents and what the
    /// `max_{rows,bytes}_to_transfer` limits count.
    if (preliminary_distinct && !usesExternalTable() && pipeline.getNumStreams() > 1)
    {
        /// With `transform_null_in = 0` the set fill skips rows with a NULL in any key component, so
        /// the preliminary deduplication drops them too instead of hashing and counting them.
        const auto & input_header = *getInputHeaders().front();
        const bool skip_null_keys = !set_and_key->set->transformNullIn()
            && std::any_of(
                input_header.begin(), input_header.end(), [](const auto & col) { return isNullableOrLowCardinalityNullable(col.type); });

        pipeline.addSimpleTransform(
            [&](const SharedHeader & header, QueryPipelineBuilder::StreamType stream_type) -> ProcessorPtr
            {
                if (stream_type != QueryPipelineBuilder::StreamType::Main)
                    return nullptr;

                /// Deduplicate independently per stream. The set fill deduplicates anyway, so on
                /// mostly-unique input the transform may abandon and pass rows through.
                return std::make_shared<DistinctTransform>(
                    header, SizeLimits{}, 0, Names{}, /*allow_abandoning_=*/true, skip_null_keys);
            });
    }

    pipeline.addCreatingSetsTransform(
        getOutputHeader(),
        set_and_key,
        network_transfer_limits,
        prepared_sets_cache);
}

void CreatingSetStep::updateOutputHeader()
{
    output_header = std::make_shared<const Block>(Block{});
}

void CreatingSetStep::describeActions(FormatSettings & settings) const
{
    if (!set_and_key->set)
        return ;

    const String & prefix = settings.detail_prefix;

    settings.out << prefix;
    settings.out << "Set: ";
    settings.out << (settings.pretty ? QueryPlanFormat::formatColumnPretty(set_and_key->key, settings.pretty_names) : set_and_key->key) << '\n';

    if (preliminary_distinct)
        settings.out << prefix << "Pre-distinct: 1\n";
}

void CreatingSetStep::describeActions(JSONBuilder::JSONMap & map) const
{
    if (set_and_key->set)
        map.add("Set", set_and_key->key);
    if (preliminary_distinct)
        map.add("Pre-distinct", true);
}


CreatingSetsStep::CreatingSetsStep(SharedHeaders input_headers_)
{
    if (input_headers_.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CreatingSetsStep cannot be created with no inputs");

    input_headers = std::move(input_headers_);
    output_header = input_headers.front();

    for (size_t i = 1; i < input_headers.size(); ++i)
        if (!input_headers[i]->empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Creating set input must have empty header. Got: {}",
                            input_headers[i]->dumpStructure());
}

QueryPipelineBuilderPtr CreatingSetsStep::updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings &)
{
    if (pipelines.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CreatingSetsStep cannot be created with no inputs");

    auto main_pipeline = std::move(pipelines.front());
    if (pipelines.size() == 1)
        return main_pipeline;

    pipelines.erase(pipelines.begin());

    QueryPipelineBuilder delayed_pipeline;
    if (pipelines.size() > 1)
    {
        QueryPipelineProcessorsCollector collector(delayed_pipeline, this);
        delayed_pipeline = QueryPipelineBuilder::unitePipelines(std::move(pipelines));
        processors = collector.detachProcessors();
    }
    else
        delayed_pipeline = std::move(*pipelines.front());

    QueryPipelineProcessorsCollector collector(*main_pipeline, this);
    main_pipeline->addPipelineBefore(std::move(delayed_pipeline));
    auto added_processors = collector.detachProcessors();
    processors.insert(processors.end(), added_processors.begin(), added_processors.end());

    return main_pipeline;
}

void CreatingSetsStep::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}

void addCreatingSetsStep(QueryPlan & query_plan, PreparedSets::Subqueries subqueries, ContextPtr context)
{
    SharedHeaders input_headers;
    input_headers.emplace_back(query_plan.getCurrentHeader());

    std::vector<std::unique_ptr<QueryPlan>> plans;
    plans.emplace_back(std::make_unique<QueryPlan>(std::move(query_plan)));
    query_plan = QueryPlan();

    const auto & settings = context->getSettingsRef();
    SizeLimits network_transfer_limits(settings[Setting::max_rows_to_transfer], settings[Setting::max_bytes_to_transfer], settings[Setting::transfer_overflow_mode]);
    auto prepared_sets_cache = context->getPreparedSetsCache();
    for (auto & future_set : subqueries)
    {
        if (future_set->get())
            continue;

        auto plan = future_set->build(network_transfer_limits, prepared_sets_cache);
        if (!plan)
            continue;

        input_headers.emplace_back(plan->getCurrentHeader());
        plans.emplace_back(std::move(plan));
    }

    if (plans.size() == 1)
    {
        query_plan = std::move(*plans.front());
        return;
    }

    auto creating_sets = std::make_unique<CreatingSetsStep>(std::move(input_headers));
    creating_sets->setStepDescription("Create sets before main query execution");
    query_plan.unitePlans(std::move(creating_sets), std::move(plans));
}

QueryPipelineBuilderPtr addCreatingSetsTransform(QueryPipelineBuilderPtr pipeline, PreparedSets::Subqueries subqueries, ContextPtr context)
{
    SharedHeaders input_headers;
    input_headers.emplace_back(pipeline->getSharedHeader());

    QueryPipelineBuilders pipelines;
    pipelines.reserve(1 + subqueries.size());
    pipelines.push_back(std::move(pipeline));

    QueryPlanOptimizationSettings plan_settings(context);
    BuildQueryPipelineSettings pipeline_settings(context);

    const auto & settings = context->getSettingsRef();
    SizeLimits network_transfer_limits(settings[Setting::max_rows_to_transfer], settings[Setting::max_bytes_to_transfer], settings[Setting::transfer_overflow_mode]);
    auto prepared_sets_cache = context->getPreparedSetsCache();

    for (auto & future_set : subqueries)
    {
        if (future_set->get())
            continue;

        auto plan = future_set->build(network_transfer_limits, prepared_sets_cache);
        if (!plan)
            continue;

        input_headers.emplace_back(plan->getCurrentHeader());
        pipelines.emplace_back(plan->buildQueryPipeline(plan_settings, pipeline_settings));
    }

    return CreatingSetsStep(input_headers).updatePipeline(std::move(pipelines), pipeline_settings);
}

std::vector<std::unique_ptr<QueryPlan>> DelayedCreatingSetsStep::makePlansForSets(
    DelayedCreatingSetsStep && step,
    const QueryPlanOptimizationSettings & optimization_settings)
{
    std::vector<std::unique_ptr<QueryPlan>> plans;

    for (auto & future_set : step.subqueries)
    {
        if (future_set->get())
            continue;

        auto plan = future_set->build(optimization_settings.network_transfer_limits, optimization_settings.prepared_sets_cache);
        if (!plan)
            continue;

        /// The set's plan was built by the Planner under
        /// `forceMaterializeCTE`, which plants a safety-net
        /// `DelayedMaterializingCTEsStep` per dependency level on the
        /// source plan so that `buildSetInplace` / `buildOrderedSetInplace`
        /// can materialize the referenced CTEs synchronously if that path
        /// fires first. Here we are attaching the plan for *runtime* set
        /// construction; for the runtime path, we want the outer plan's
        /// `MaterializingCTEsStep` to be the single canonical writer site
        /// so its `DelayedPortsProcessor` lazily gates every reader,
        /// including readers that sit on the "main" (always-eventually-pulled)
        /// side of an inner `CreatingSets` gate. The outer plan can only
        /// win that role if no surviving `DelayedMaterializingCTEsStep`
        /// inside this sub-plan claims `is_materialization_planned` first
        /// via the recursive `plan->optimize(...)` below — that includes
        /// per-branch safety-nets planted by `buildPlanForQueryNode` below
        /// each `UnionStep` / `IntersectOrExceptStep` branch, which sit
        /// *below* the union-level safety-net.
        ///
        /// So strip every `DelayedMaterializingCTEsStep` in this sub-plan
        /// tree, not just the top contiguous chain. Nested
        /// `DelayedCreatingSetsStep` source plans (held in
        /// `subqueries`, not as children of any node in this tree) are
        /// untouched — they keep their safety-nets for their own
        /// `buildSetInplace` / `buildOrderedSetInplace` consumers.
        removeAllDelayedMaterializingCTEsStep(*plan);

        plan->optimize(optimization_settings);
        plans.emplace_back(std::move(plan));
    }

    return plans;
}

void addDelayedCreatingSetsStep(QueryPlan & query_plan, PreparedSetsPtr prepared_sets, ContextPtr context)
{
    if (!prepared_sets)
        return;

    auto subqueries = prepared_sets->getSubqueries();
    if (subqueries.empty())
        return;

    const auto & settings = context->getSettingsRef();
    SizeLimits network_transfer_limits(settings[Setting::max_rows_to_transfer], settings[Setting::max_bytes_to_transfer], settings[Setting::transfer_overflow_mode]);
    auto prepared_sets_cache = context->getPreparedSetsCache();

    auto step = std::make_unique<DelayedCreatingSetsStep>(
            query_plan.getCurrentHeader(),
            std::move(subqueries),
            network_transfer_limits,
            prepared_sets_cache);

    query_plan.addStep(std::move(step));
}

DelayedCreatingSetsStep::DelayedCreatingSetsStep(
    SharedHeader input_header,
    PreparedSets::Subqueries subqueries_,
    SizeLimits network_transfer_limits_,
    PreparedSetsCachePtr prepared_sets_cache_)
    : subqueries(std::move(subqueries_))
    , network_transfer_limits(std::move(network_transfer_limits_))
    , prepared_sets_cache(std::move(prepared_sets_cache_))
{
    input_headers = {input_header};
    output_header = std::move(input_header);
}

QueryPipelineBuilderPtr DelayedCreatingSetsStep::updatePipeline(QueryPipelineBuilders, const BuildQueryPipelineSettings &)
{
    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "Cannot build pipeline in DelayedCreatingSets. This step should be optimized out.");
}

void forEachSubquerySet(const QueryPlan * root, const std::function<bool(FutureSetFromSubquery &)> & visit)
{
    if (!root || !root->getRootNode())
        return;

    std::vector<QueryPlan::Node *> stack{root->getRootNode()};
    while (!stack.empty())
    {
        auto * node = stack.back();
        stack.pop_back();

        if (auto * delayed = typeid_cast<DelayedCreatingSetsStep *>(node->step.get()))
        {
            for (const auto & future_set : delayed->getSets())
            {
                if (!future_set)
                    continue;
                if (visit(*future_set))
                    forEachSubquerySet(future_set->getQueryPlan(), visit);
            }
        }
        else if (auto * read_from_local = typeid_cast<ReadFromLocalParallelReplicaStep *>(node->step.get()))
        {
            forEachSubquerySet(read_from_local->getQueryPlan(), visit);
        }

        /// Deliberately NOT descending into `node->step->getChildPlans()`, unlike the other plan-wide
        /// walks (`hasCorrelatedExpressions`, `DistributedPlanSets`). No set is missed by stopping
        /// here, for a different reason per carrier.
        ///
        /// `ReadFromMerge` and `LazyReadReplacingFinalStep` build their child plans when asked for
        /// them rather than handing them out, and paying for that here loses more than the sharing
        /// wins: on `SELECT ... WHERE key IN (SELECT ... FROM <merge table>)` under automatic parallel
        /// replicas it made the nested subquery run one extra time rather than one time fewer. Both
        /// are also unsupported for dataflow statistics collection, which keeps the whole optimization
        /// off the plan they appear in.
        ///
        /// `JoinStepLogicalLookup` is cheap to ask, but its child plan cannot hold a set at all: the
        /// step is built only for a right side whose `useful_sets` is empty (see `PlannerJoinTree`),
        /// and a set on that side is exactly what makes it non-empty.
        for (auto * child : node->children)
            stack.push_back(child);
    }
}

/// Note that on the single-node plan this only ever reaches the sets held directly by a
/// `DelayedCreatingSetsStep`, never the ones a nested `IN` keeps below them: by the time automatic
/// parallel replicas runs, every set here has been through `FutureSetFromSubquery::build`, which moves
/// the source plan out, so `getQueryPlan` returns null and the recursion stops (measured for both
/// values of `use_index_for_in_with_subqueries`, with the `IN` on an indexed and on a plain column).
/// A nested set is therefore not shareable through any walk - it exists only inside a plan that no
/// longer hangs off the set. The recursion still pays off on the probe plan in `reuseBuiltSets`, which
/// runs before that plan is optimized and so still has its source plans.
BuiltSetsByHashPtr collectBuiltSets(const QueryPlan & plan)
{
    auto built = std::make_shared<BuiltSetsByHash>();
    forEachSubquerySet(
        &plan,
        [&](FutureSetFromSubquery & future_set)
        {
            const auto & set_and_key = future_set.getSetAndKey();
            /// Sets without explicit elements are shared too. `buildOrderedSetInplace` returns such a set
            /// as-is, so the probe plan cannot build elements from its own source and its selectivity
            /// analysis for that `IN` falls back to a default estimate. That is cheaper than the
            /// alternative: withholding the set makes the probe re-execute the subquery just to plan a
            /// candidate that is often discarded, and the accepted plan gets the built set anyway from
            /// `moveSetsFromLocalPlanToReplicasPlan`.
            if (set_and_key && set_and_key->set && set_and_key->set->isCreated())
                built->sets.emplace(future_set.getHash(), set_and_key);
            return true;
        });
    return built;
}

void reuseBuiltSets(QueryPlan & plan, const BuiltSetsByHashPtr & built)
{
    if (!built || built->sets.empty())
        return;

    forEachSubquerySet(
        &plan,
        [&](FutureSetFromSubquery & future_set)
        {
            const auto & set_and_key = future_set.getSetAndKey();
            if (set_and_key && set_and_key->set && set_and_key->set->isCreated())
                return false;

            if (auto it = built->sets.find(future_set.getHash()); it != built->sets.end())
            {
                future_set.replaceSetAndKey(it->second);
                return false;
            }

            /// Not adopted, so this set still builds from its own source plan, and the sets nested in
            /// that plan are live too.
            return true;
        });
}

}
