#include <Processors/QueryPlan/ReadFromMemoryStorageStep.h>

#include <Analyzer/TableNode.h>

#include <Common/Exception.h>
#include <Common/JSONBuilder.h>
#include <Common/typeid_cast.h>

#include <Core/Settings.h>

#include <Columns/FilterDescription.h>
#include <Formats/FormatFilterInfo.h>
#include <Functions/FunctionTopKFilter.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/getColumnFromBlock.h>
#include <Interpreters/inplaceBlockConversions.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/MaterializedCTE.h>
#include <Storages/MergeTree/MergeTreeSplitPrewhereIntoReadSteps.h>
#include <Storages/StorageSnapshot.h>
#include <Storages/StorageMemory.h>
#include <Storages/VirtualColumnUtils.h>

#include <IO/Operators.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/ISource.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <Processors/Sources/NullSource.h>

#include <atomic>
#include <functional>
#include <memory>

#include <fmt/ranges.h>

namespace DB
{

namespace Setting
{

extern const SettingsBool enable_multiple_prewhere_read_steps;

}

namespace ErrorCodes
{

extern const int LOGICAL_ERROR;

}

/// In-source filtering for the TopN threshold, the row-level security filter and PREWHERE.
/// The steps are applied to every stored block one after another, and each step reads its columns
/// only for the rows that passed the previous steps. A conjunction in PREWHERE is split into several
/// steps, like `MergeTreeSelectProcessor` does with `enable_multiple_prewhere_read_steps`, with the
/// conditions over the cheapest columns first. So for a table with `compress = true` a selective
/// condition only decompresses the columns it uses, and blocks where no row passes are skipped
/// without touching the columns of the later steps and the rest of the query.
struct MemorySourceFilter
{
    struct Step
    {
        ExpressionActionsPtr actions;
        String filter_column_name;
        /// A filter column that is kept holds its own values for the passing rows, as in
        /// `MergeTreeRangeReader`. It must not be replaced by a constant, like the header in
        /// `SourceStepWithFilter::applyPrewhereActions` is: for `PREWHERE k` it is the column `k`
        /// itself, which is not read again.
        bool remove_filter_column = false;
        /// The requested physical columns that the step's actions take as input.
        /// Those not produced by the previous steps are read before the step is executed.
        NamesAndTypesList input_columns;
    };

    std::vector<Step> steps;

    /// The columns of the output header that are requested physical columns, in header order.
    /// Those not produced by the steps are read after all steps, only for the passing rows.
    NamesAndTypesList output_columns;
};

using MemorySourceFilterPtr = std::shared_ptr<const MemorySourceFilter>;

class MemorySource : public ISource
{
    using InitializerFunc = std::function<void(std::shared_ptr<const Blocks> &)>;

    static Block getHeader(const NamesAndTypesList & physical, const NamesAndTypesList & virtuals)
    {
        Block res;
        for (const auto & name_type : physical)
            res.insert({name_type.type->createColumn(), name_type.type, name_type.name});
        for (const auto & name_type : virtuals)
            res.insert({name_type.type->createColumn(), name_type.type, name_type.name});
        return res;
    }

public:
    MemorySource(
        NamesAndTypesList physical_columns_,
        NamesAndTypesList virtual_columns_,
        std::shared_ptr<const Blocks> data_,
        std::shared_ptr<std::atomic<size_t>> parallel_execution_index_,
        InitializerFunc initializer_func_ = {},
        MaterializedCTEPtr materialized_cte_ = {},
        MemorySourceFilterPtr filter_ = {},
        SharedHeader filtered_header_ = {})
        : ISource(filter_ ? filtered_header_ : std::make_shared<const Block>(getHeader(physical_columns_, virtual_columns_)))
        , physical_columns(std::move(physical_columns_))
        , virtual_columns(std::move(virtual_columns_))
        , data(data_)
        , parallel_execution_index(parallel_execution_index_)
        , initializer_func(std::move(initializer_func_))
        , materialized_cte(std::move(materialized_cte_))
        , filter(std::move(filter_))
    {
        if (filter && !virtual_columns.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown virtual columns: '{}'", virtual_columns.getNames());
    }

    String getName() const override { return "Memory"; }

protected:
    Chunk generate() override
    {
        if (initializer_func)
        {
            if (materialized_cte)
            {
                /// Fail-fast invariant: by the time `MemorySource::generate`
                /// runs, `DelayedPortsProcessor` (inserted by
                /// `MaterializingCTEsStep::updatePipeline` via
                /// `addPipelineBefore`) has already gated this reader on the
                /// corresponding `MaterializingCTETransform` finishing. If we
                /// observe `is_built == false` here, the planner failed to
                /// wire the gate - fail loudly rather than read from a
                /// half-populated `StorageMemory`.
                if (!materialized_cte->is_built.load(std::memory_order_acquire))
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Reading from materialized CTE '{}' before its materialization completed - "
                        "DelayedPortsProcessor gate is missing in the query plan",
                        materialized_cte->cte_name);
            }

            initializer_func(data);
            initializer_func = {};
        }

        while (true)
        {
            size_t current_index = getAndIncrementExecutionIndex();

            if (!data || current_index >= data->size())
                return {};

            const Block & src = (*data)[current_index];

            if (filter)
            {
                if (auto chunk = generateFiltered(src))
                    return std::move(*chunk);

                /// Every row of this block was filtered out; move on to the next block
                /// without reading the rest of its columns. `generateFiltered` has already
                /// reported the block's rows as read.
                if (isCancelled())
                    return {};
                continue;
            }

            Columns columns;
            columns.reserve(physical_columns.size() + virtual_columns.size());
            fillPhysicalColumns(src, columns);

            UInt64 num_rows = columns.empty() ? 0 : columns.front()->size();
            if (!columns.empty())
                fillVirtualColumns(columns, num_rows);

            return Chunk(std::move(columns), num_rows);
        }
    }

private:
    size_t getAndIncrementExecutionIndex()
    {
        if (parallel_execution_index)
        {
            return (*parallel_execution_index)++;
        }

        return execution_index++;
    }

    static ColumnPtr readColumn(const Block & src, const NameAndTypePair & name_and_type)
    {
        if (name_and_type.isSubcolumn())
            return tryGetSubcolumnFromBlock(src, name_and_type.getTypeInStorage(), name_and_type);
        return tryGetColumnFromBlock(src, name_and_type);
    }

    void fillPhysicalColumns(const Block & src, Columns & result_columns) const
    {
        for (const auto & name_and_type : physical_columns)
            result_columns.emplace_back(readColumn(src, name_and_type));

        fillMissingColumns(result_columns, src.rows(), physical_columns, physical_columns, {}, nullptr);
        chassert(std::all_of(result_columns.begin(), result_columns.end(), [](const auto & column) { return column != nullptr; }));
    }

    /// Applies the filter steps (row-level security filter, PREWHERE) to one stored block.
    /// Returns std::nullopt when no row passes.
    ///
    /// Reports the read progress explicitly, because the automatic accounting of `ISource` uses
    /// the returned chunk, which here holds only the rows that passed the filter - and nothing at
    /// all for a block that the filter eliminated completely. The reported number of rows is the
    /// number of rows scanned, the same as what `ReadFromMergeTree` reports for its `PREWHERE`,
    /// so `max_rows_to_read`, read quotas and `SelectedRows` still see the whole scan.
    ///
    /// The layout of the block after the steps depends on how the steps were split, so the result
    /// is assembled by name in the order of the output header, as in `MergeTreeSelectProcessor`.
    std::optional<Chunk> generateFiltered(const Block & src)
    {
        const size_t num_src_rows = src.rows();

        /// The size of the columns materialized from this block, accumulated as they are read.
        size_t num_read_bytes = 0;

        Block block;
        size_t num_rows = num_src_rows;

        /// Mask over the stored block's rows combining all steps so far, for cutting the columns
        /// read after some step has filtered. Empty while no step has filtered anything.
        IColumn::Filter combined_mask;

        /// Reads those of the columns that the block does not have yet, only for the rows that
        /// passed the steps so far.
        auto read_missing_columns = [&](const NamesAndTypesList & columns_to_read)
        {
            NamesAndTypesList missing;
            for (const auto & name_and_type : columns_to_read)
                if (!block.has(name_and_type.name))
                    missing.push_back(name_and_type);

            if (missing.empty())
                return;

            Columns columns;
            columns.reserve(missing.size());
            for (const auto & name_and_type : missing)
                columns.emplace_back(readColumn(src, name_and_type));

            fillMissingColumns(columns, num_src_rows, missing, missing, {}, nullptr);

            auto column_it = columns.begin();
            for (const auto & name_and_type : missing)
            {
                ColumnPtr column = std::move(*column_it);
                ++column_it;
                num_read_bytes += column->byteSize();
                if (!combined_mask.empty())
                    column = column->filter(combined_mask, num_rows);
                block.insert({column, name_and_type.type, name_and_type.name});
            }
        };

        for (const auto & step : filter->steps)
        {
            read_missing_columns(step.input_columns);
            step.actions->execute(block, num_rows);

            const size_t filter_column_position = block.getPositionByName(step.filter_column_name);
            ColumnPtr filter_column = block.getByPosition(filter_column_position).column;

            ConstantFilterDescription constant_filter(*filter_column);
            if (constant_filter.always_false)
            {
                progress(num_src_rows, num_read_bytes);
                return std::nullopt;
            }

            if (!constant_filter.always_true)
            {
                FilterDescription filter_description(*filter_column);
                const size_t num_passed_rows = filter_description.countBytesInFilter();
                if (num_passed_rows == 0)
                {
                    progress(num_src_rows, num_read_bytes);
                    return std::nullopt;
                }

                if (num_passed_rows != num_rows)
                {
                    for (auto & elem : block)
                        elem.column = filter_description.filter(*elem.column, num_passed_rows);

                    if (combined_mask.empty())
                    {
                        combined_mask.assign(*filter_description.data);
                    }
                    else
                    {
                        /// This step's mask indexes the rows that passed the previous steps.
                        size_t pos = 0;
                        for (auto & passed : combined_mask)
                            if (passed)
                                passed = (*filter_description.data)[pos++];
                        chassert(pos == filter_description.data->size());
                    }
                }

                num_rows = num_passed_rows;
            }

            if (step.remove_filter_column)
                block.erase(filter_column_position);
        }

        read_missing_columns(filter->output_columns);

        progress(num_src_rows, num_read_bytes);

        const auto & header = getPort().getHeader();
        Columns result_columns;
        result_columns.reserve(header.columns());
        for (const auto & elem : header)
            result_columns.push_back(block.getByName(elem.name).column);

        return Chunk(std::move(result_columns), num_rows);
    }

    void fillVirtualColumns([[maybe_unused]] Columns & result_columns, [[maybe_unused]] UInt64 num_rows) const
    {
        if (!virtual_columns.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown virtual columns: '{}'", virtual_columns.getNames());
    }

    const NamesAndTypesList physical_columns;
    const NamesAndTypesList virtual_columns;
    size_t execution_index = 0;
    std::shared_ptr<const Blocks> data;
    std::shared_ptr<std::atomic<size_t>> parallel_execution_index;
    InitializerFunc initializer_func;
    MaterializedCTEPtr materialized_cte;
    MemorySourceFilterPtr filter;
};

ReadFromMemoryStorageStep::ReadFromMemoryStorageStep(
    const Names & columns_to_read_,
    const SelectQueryInfo & query_info_,
    const StorageSnapshotPtr & storage_snapshot_,
    const ContextPtr & context_,
    StoragePtr storage_,
    const size_t num_streams_,
    const bool delay_read_for_global_sub_queries_)
    : SourceStepWithFilter(
        /// `query_info` may already carry PREWHERE (an explicit `PREWHERE` clause) and a pushed-down
        /// row-level security filter; they are applied inside the source, so the output header must
        /// reflect them. This is a no-op when both are absent.
        std::make_shared<const Block>(SourceStepWithFilter::applyPrewhereActions(
            storage_snapshot_->getSampleBlockForColumns(columns_to_read_),
            query_info_.row_level_filter,
            query_info_.prewhere_info)),
        columns_to_read_,
        query_info_,
        storage_snapshot_,
        context_)
    , columns_to_read(columns_to_read_)
    , storage(std::move(storage_))
    , num_streams(num_streams_)
    , delay_read_for_global_sub_queries(delay_read_for_global_sub_queries_)
{
}

void ReadFromMemoryStorageStep::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    auto pipe = makePipe();

    if (pipe.empty())
    {
        pipe = Pipe(std::make_shared<NullSource>(output_header));
    }

    pipeline.init(std::move(pipe));
}

QueryPlanStepPtr ReadFromMemoryStorageStep::clone() const
{
    return std::make_unique<ReadFromMemoryStorageStep>(*this);
}

void ReadFromMemoryStorageStep::applyFilters(ActionDAGNodes added_filter_nodes)
{
    SourceStepWithFilter::applyFilters(std::move(added_filter_nodes));

    /// The row-level filter and `PREWHERE` are evaluated inside the source, which starts running
    /// as soon as the pipeline is executed. A condition such as `PREWHERE k IN (SELECT ...)` carries
    /// a `FutureSet` that the pipeline-level `CreatingSetsStep` fills in; relying on it is not
    /// enough, because `DelayedPortsProcessor` can be short-circuited by a downstream processor that
    /// closes its inputs early, and the source would then see a not-ready set. Build the sets in
    /// place, the same way `ReadFromMergeTree` does for its storage-level `PREWHERE`.
    /// This has to happen here, during plan optimization: at the end of `QueryPlan::optimize`,
    /// `DelayedCreatingSetsStep` takes the subquery plans out of the sets, and an in-place build
    /// from `initializePipeline` would find nothing to execute.
    /// Sets of `GLOBAL IN` in an explicit `PREWHERE` or a row policy are built here too; only the
    /// optimizer-moved condition in `updatePrewhereInfo` has to leave them out.
    if (query_info.row_level_filter)
        VirtualColumnUtils::buildSetsForDAG(query_info.row_level_filter->actions, context);
    if (query_info.prewhere_info)
        VirtualColumnUtils::buildSetsForDAG(query_info.prewhere_info->prewhere_actions, context);

    filters_applied = true;
}

void ReadFromMemoryStorageStep::updatePrewhereInfo(const PrewhereInfoPtr & prewhere_info_value)
{
    SourceStepWithFilter::updatePrewhereInfo(prewhere_info_value);

    /// `optimizePrewhere` runs after `applyFilters`, so a condition with `IN (subquery)` that it moves
    /// into `PREWHERE` still needs its set built in place, for the reason given in `applyFilters`.
    /// Only when `applyFilters` did run for this step: otherwise the sets are left to the
    /// `CreatingSetsStep` of the plan, and building one here would execute the subquery twice.
    /// Sets of `GLOBAL IN` are excluded: `ReadFromRemote` may still have to attach an external table
    /// to them, which fails on a set that is already built.
    if (filters_applied && query_info.prewhere_info)
        VirtualColumnUtils::buildSetsForDAGExcludingGlobalIn(query_info.prewhere_info->prewhere_actions, context);
}

bool ReadFromMemoryStorageStep::supportsTopKDynamicFilter(const ColumnWithTypeAndName & sort_column) const
{
    /// Virtual columns do not qualify.
    if (std::ranges::find(columns_to_read, sort_column.name) == columns_to_read.end())
        return false;

    const auto column = storage_snapshot->tryGetColumn(GetColumnsOptions(GetColumnsOptions::AllPhysical).withSubcolumns(), sort_column.name);
    if (!column)
        return false;

    /// The source fills a column that a block does not have (e.g. one added by `ALTER TABLE ADD COLUMN`
    /// after the block was inserted) with the defaults of the type. For a column without a default
    /// expression these are exactly the values the query sees. For a column with a `DEFAULT` expression
    /// the values agree only as long as nothing evaluates the expression above the source; do not rely
    /// on that and exclude such columns and their subcolumns, the same way
    /// `StorageMemory::supportedPrewhereColumns` does for `PREWHERE`.
    return !storage_snapshot->metadata->getColumns().hasDefault(column->getNameInStorage());
}

void ReadFromMemoryStorageStep::setTopKFilter(FormatTopKFilterInfoPtr info)
{
    top_k_filter = std::move(info);
}

void ReadFromMemoryStorageStep::describeActions(FormatSettings & format_settings) const
{
    SourceStepWithFilter::describeActions(format_settings);
    if (top_k_filter)
        format_settings.out << format_settings.detail_prefix << "TopN filter column: " << top_k_filter->column_name << '\n';
}

void ReadFromMemoryStorageStep::describeActions(JSONBuilder::JSONMap & map) const
{
    SourceStepWithFilter::describeActions(map);
    if (top_k_filter)
        map.add("TopN Filter Column", top_k_filter->column_name);
}

MemorySourceFilterPtr ReadFromMemoryStorageStep::makeSourceFilter(const NamesAndTypesList & physical_columns) const
{
    /// The threshold filter runs first and shrinks the block before the other steps, which then see
    /// a different set of rows. `tryOptimizeTopK` checks that the filters do not depend on that, but
    /// `optimizePrewhere` may have moved such a condition into PREWHERE after it.
    const bool use_top_k_filter = top_k_filter
        && !(query_info.row_level_filter && query_info.row_level_filter->actions.hasNonDeterministicOrStatefulFunctions())
        && !(query_info.prewhere_info && query_info.prewhere_info->prewhere_actions.hasNonDeterministicOrStatefulFunctions());

    auto result = std::make_shared<MemorySourceFilter>();
    ExpressionActionsSettings actions_settings(context);

    /// Drop the rows that cannot enter the top-K heap of the query. The comparison is cheap and, once
    /// the threshold is set, usually the most selective of the filters, so it goes first: the columns
    /// of the later steps are then read only for the few remaining rows, and a block where no row is
    /// within the threshold is skipped after reading only the sort column.
    if (use_top_k_filter)
    {
        const auto sort_column = physical_columns.tryGetByName(top_k_filter->column_name);
        if (!sort_column)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The sort column '{}' of the TopN filter is not read", top_k_filter->column_name);

        ActionsDAG dag({*sort_column});
        const auto * input_node = dag.getInputs().front();
        const auto & filter_node = dag.addFunction(
            createInternalFunctionTopKFilterResolver(top_k_filter->threshold_tracker), {input_node}, {});
        dag.getOutputs() = {input_node, &filter_node};

        /// The steps find columns in the block by name.
        if (!physical_columns.contains(filter_node.result_name) && !output_header->has(filter_node.result_name))
        {
            String filter_column_name = filter_node.result_name;
            result->steps.push_back({
                .actions = std::make_shared<ExpressionActions>(std::move(dag), actions_settings),
                .filter_column_name = std::move(filter_column_name),
                .remove_filter_column = true,
                .input_columns = {},
            });
        }
    }

    if (result->steps.empty() && !query_info.row_level_filter && !query_info.prewhere_info)
        return nullptr;

    /// The row-level security filter runs first, so PREWHERE expressions are never evaluated
    /// on the rows the policy hides.
    if (query_info.row_level_filter)
    {
        const auto & row_level_filter = *query_info.row_level_filter;
        result->steps.push_back({
            .actions = std::make_shared<ExpressionActions>(row_level_filter.actions.clone(), actions_settings),
            .filter_column_name = row_level_filter.column_name,
            .remove_filter_column = row_level_filter.do_remove_column,
            .input_columns = {},
        });
    }

    if (query_info.prewhere_info)
    {
        /// Split a conjunction into steps, so that the columns of a later condition are read only
        /// for the rows that passed the earlier ones. The steps always filter the block, which is
        /// what their `need_filter` asks for, so it is not needed here.
        /// A stateful function (e.g. `runningConcurrency`, `rowNumberInBlock`) or a function that is
        /// non-deterministic in scope of the query (e.g. `blockSize`, `rand`) depends on the set of
        /// rows it is evaluated on, so a condition with it must see all rows, not only those that
        /// passed the preceding conditions: such a PREWHERE is evaluated in a single step.
        PrewhereExprInfo prewhere_steps;
        if (context->getSettingsRef()[Setting::enable_multiple_prewhere_read_steps]
            && !query_info.prewhere_info->prewhere_actions.hasNonDeterministicOrStatefulFunctions()
            && tryBuildPrewhereSteps(
                query_info.prewhere_info,
                actions_settings,
                prewhere_steps,
                /*force_short_circuit_execution*/ false,
                &storage_snapshot->metadata->getColumns()))
        {
            for (const auto & step : prewhere_steps.steps)
            {
                result->steps.push_back({
                    .actions = step->actions,
                    .filter_column_name = step->filter_column_name,
                    .remove_filter_column = step->remove_filter_column,
                    .input_columns = {},
                });
            }
        }
        else
        {
            const auto & prewhere_info = *query_info.prewhere_info;
            result->steps.push_back({
                .actions = std::make_shared<ExpressionActions>(prewhere_info.prewhere_actions.clone(), actions_settings),
                .filter_column_name = prewhere_info.prewhere_column_name,
                .remove_filter_column = prewhere_info.remove_prewhere_column,
                .input_columns = {},
            });
        }
    }

    for (auto & step : result->steps)
    {
        const Names required_columns = step.actions->getRequiredColumns();
        const NameSet required_column_names(required_columns.begin(), required_columns.end());
        for (const auto & name_and_type : physical_columns)
            if (required_column_names.contains(name_and_type.name))
                step.input_columns.push_back(name_and_type);
    }

    for (const auto & elem : *output_header)
        if (auto name_and_type = physical_columns.tryGetByName(elem.name))
            result->output_columns.push_back(*name_and_type);

    return result;
}

Pipe ReadFromMemoryStorageStep::makePipe()
{
    storage_snapshot->check(columns_to_read);

    auto [physical_column_names, virtual_column_names] = VirtualColumnUtils::splitPhysicalAndVirtualColumnNames(columns_to_read, storage_snapshot);
    auto physical_columns = storage_snapshot->getColumnsByNames(GetColumnsOptions(GetColumnsOptions::All).withSubcolumns(), physical_column_names);
    auto virtual_columns = storage_snapshot->getColumnsByNames(GetColumnsOptions(GetColumnsOptions::All).withVirtuals(VirtualsKind::All, VirtualsMaterializationPlace::Reader), virtual_column_names);

    auto source_filter = makeSourceFilter(physical_columns);

    const auto & snapshot_data = assert_cast<const StorageMemory::SnapshotData &>(*storage_snapshot->data);
    auto current_data = snapshot_data.blocks;

    if (delay_read_for_global_sub_queries)
    {
        /// Note: for global subquery we use single source.
        /// Mainly, the reason is that at this point table is empty,
        /// and we don't know the number of blocks are going to be inserted into it.
        ///
        /// It may seem to be not optimal, but actually data from such table is used to fill
        /// set for IN or hash table for JOIN, which can't be done concurrently.
        /// Since no other manipulation with data is done, multiple sources shouldn't give any profit.

        return Pipe(std::make_shared<MemorySource>(
            physical_columns,
            virtual_columns,
            nullptr /* data */,
            nullptr /* parallel execution index */,
            [my_storage = storage](std::shared_ptr<const Blocks> & data_to_initialize)
            {
                auto current = assert_cast<const StorageMemory &>(*my_storage).data.get();
                data_to_initialize = std::shared_ptr<const Blocks>(current, &current->blocks);
            },
            typeid_cast<StorageMemory *>(storage.get())->getMaterializedCTE(),
            source_filter,
            output_header));
    }

    size_t size = current_data->size();
    num_streams = std::min(num_streams, size);
    Pipes pipes;

    auto parallel_execution_index = std::make_shared<std::atomic<size_t>>(0);

    for (size_t stream = 0; stream < num_streams; ++stream)
    {
        auto source = std::make_shared<MemorySource>(
            physical_columns, virtual_columns, current_data, parallel_execution_index, nullptr, nullptr, source_filter, output_header);
        if (stream == 0)
            source->addTotalRowsApprox(snapshot_data.rows);
        pipes.emplace_back(std::move(source));
    }
    return Pipe::unitePipes(std::move(pipes));
}

}
