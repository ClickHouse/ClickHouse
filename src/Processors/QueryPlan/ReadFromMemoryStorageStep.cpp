#include <Processors/QueryPlan/ReadFromMemoryStorageStep.h>

#include <Analyzer/TableNode.h>

#include <Common/Exception.h>
#include <Common/typeid_cast.h>

#include <Columns/FilterDescription.h>
#include <Databases/enableAllExperimentalSettings.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/getColumnFromBlock.h>
#include <Interpreters/inplaceBlockConversions.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/MaterializedCTE.h>
#include <Parsers/IAST.h>
#include <Storages/StorageSnapshot.h>
#include <Storages/StorageMemory.h>
#include <Storages/VirtualColumnUtils.h>

#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/ISource.h>
#include <Processors/Sources/NullSource.h>

#include <atomic>
#include <functional>
#include <map>
#include <memory>
#include <mutex>

#include <fmt/ranges.h>

namespace DB
{

namespace ErrorCodes
{

extern const int LOGICAL_ERROR;

}

/// In-source filtering for the row-level security filter and PREWHERE.
/// The steps are applied to every stored block before the block's remaining columns are read,
/// so for a table with `compress = true` a selective condition only decompresses the columns
/// it uses, and blocks where no row passes are skipped without touching the other columns.
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
    };

    std::vector<Step> steps;

    /// The requested physical columns, partitioned by whether some step consumes them.
    /// Both lists preserve the requested order.
    NamesAndTypesList filter_input_columns;
    NamesAndTypesList deferred_columns;
};

using MemorySourceFilterPtr = std::shared_ptr<const MemorySourceFilter>;

/// What the source needs to evaluate the default expressions of the requested columns that a stored block lacks
/// (the block was written before `ALTER TABLE ... ADD COLUMN`).
struct MemorySourceDefaults
{
    StorageSnapshotPtr storage_snapshot;
    ContextPtr context;
    /// The stored columns that the default expressions of the table name.
    NamesAndTypesList stored_inputs;

    struct Evaluation
    {
        ExpressionActionsPtr actions; /// null: nothing to execute
        NamesAndTypesList stored_inputs_to_read;
    };

    /// Shared by all sources of the step, so that a stateless default is analyzed once per header shape and has one value
    /// for the whole read (e.g. `now()`).
    mutable std::mutex mutex;
    mutable std::map<std::tuple<size_t, std::vector<bool>, std::vector<bool>>, Evaluation> evaluations TSA_GUARDED_BY(mutex);
};

using MemorySourceDefaultsPtr = std::shared_ptr<const MemorySourceDefaults>;

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
        SharedHeader filtered_header_ = {},
        MemorySourceDefaultsPtr defaults_ = {})
        : ISource(filter_ ? filtered_header_ : std::make_shared<const Block>(getHeader(physical_columns_, virtual_columns_)))
        , physical_columns(std::move(physical_columns_))
        , virtual_columns(std::move(virtual_columns_))
        , data(data_)
        , parallel_execution_index(parallel_execution_index_)
        , initializer_func(std::move(initializer_func_))
        , materialized_cte(std::move(materialized_cte_))
        , filter(std::move(filter_))
        , defaults(std::move(defaults_))
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
            size_t read_bytes = 0;
            fillPhysicalColumns(src, columns, read_bytes);

            UInt64 num_rows = columns.empty() ? 0 : columns.front()->size();
            if (!columns.empty())
                fillVirtualColumns(columns, num_rows);

            /// Reported explicitly, so that the inputs read to evaluate a default are counted too.
            progress(num_rows, read_bytes);
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

    /// Reads `columns` from the stored block, cut by `mask`, with a null column for each one the block lacks.
    static Columns readStoredColumns(
        const Block & src, const NamesAndTypesList & columns, const IColumn::Filter * mask, size_t num_rows, size_t & read_bytes)
    {
        Columns result;
        result.reserve(columns.size());
        for (const auto & name_and_type : columns)
        {
            auto column = readColumn(src, name_and_type);
            if (column)
            {
                read_bytes += column->byteSize();
                if (mask)
                    column = column->filter(*mask, num_rows);
            }
            result.emplace_back(std::move(column));
        }
        return result;
    }

    void fillPhysicalColumns(const Block & src, Columns & result_columns, size_t & read_bytes)
    {
        result_columns = readColumns(src, physical_columns, 0, nullptr, src.rows(), read_bytes);
    }

    /// Reads `columns` from the stored block, only the rows selected by `mask` (all rows if it is null), `num_rows` of them.
    /// A column the block lacks gets its `DEFAULT` or `MATERIALIZED` expression evaluated over those rows, like `MergeTree`
    /// does for a part without the column, or the default value of its type. Adds the size of what it materializes to `read_bytes`.
    Columns readColumns(
        const Block & src, const NamesAndTypesList & columns, size_t columns_id, const IColumn::Filter * mask, size_t num_rows, size_t & read_bytes)
    {
        Columns result = readStoredColumns(src, columns, mask, num_rows, read_bytes);

        std::vector<bool> was_read(result.size());
        for (size_t i = 0; i < result.size(); ++i)
            was_read[i] = result[i] != nullptr;

        /// With the snapshot, the columns that have a default expression are left for `evaluateDefaults`.
        fillMissingColumns(result, num_rows, columns, columns, {}, defaults ? defaults->storage_snapshot : nullptr);

        bool has_missing = false;
        for (size_t i = 0; i < result.size(); ++i)
        {
            if (!result[i])
                has_missing = true;
            else if (!was_read[i])
                read_bytes += result[i]->byteSize(); /// filled with the default value of its type
        }

        if (has_missing)
            evaluateDefaults(src, columns, columns_id, mask, num_rows, result, read_bytes);

        chassert(std::all_of(result.begin(), result.end(), [](const auto & column) { return column != nullptr; }));
        return result;
    }

    /// Fills the null entries of `result` with the default expressions of their columns, evaluated over the rows of `src`
    /// selected by `mask`.
    void evaluateDefaults(
        const Block & src,
        const NamesAndTypesList & columns,
        size_t columns_id,
        const IColumn::Filter * mask,
        size_t num_rows,
        Columns & result,
        size_t & read_bytes)
    {
        chassert(defaults);

        Block block;
        NamesAndTypesList required;
        NameSet required_names;
        std::vector<bool> is_missing(result.size());
        auto it = columns.begin();
        for (size_t i = 0; i < result.size(); ++i, ++it)
        {
            if (result[i])
            {
                if (required_names.emplace(it->name).second)
                    required.emplace_back(it->name, it->type);
                block.insert({result[i], it->type, it->name});
            }
            else
            {
                /// A subcolumn is extracted from its evaluated column in storage.
                is_missing[i] = true;
                if (required_names.emplace(it->getNameInStorage()).second)
                    required.emplace_back(it->getNameInStorage(), it->getTypeInStorage());
            }
        }

        const auto & stored_inputs = defaults->stored_inputs;
        std::vector<bool> is_input_provided;
        is_input_provided.reserve(stored_inputs.size());
        for (const auto & input : stored_inputs)
            is_input_provided.push_back(!block.has(input.name) && src.has(input.name));

        /// The key determines the header of the evaluation: the column list, which of its columns are read, and which
        /// stored inputs the block provides.
        auto key = std::make_tuple(columns_id, is_missing, is_input_provided);

        const auto & cache = *defaults;
        const MemorySourceDefaults::Evaluation * evaluation = nullptr;
        MemorySourceDefaults::Evaluation uncached_evaluation;
        {
            std::lock_guard lock(cache.mutex);
            if (auto cached = cache.evaluations.find(key); cached != cache.evaluations.end())
            {
                evaluation = &cached->second;
            }
            else
            {
                Block header = block.cloneEmpty();
                auto input_it = stored_inputs.begin();
                for (size_t i = 0; i < stored_inputs.size(); ++i, ++input_it)
                    if (is_input_provided[i])
                        header.insert({input_it->type->createColumn(), input_it->type, input_it->name});

                /// The expressions are resolved against a table of the header's columns, which cannot be empty. A stored
                /// column that no default names is never an input of the actions.
                if (!header.columns())
                {
                    for (const auto & stored_column : src)
                    {
                        if (!required_names.contains(stored_column.name))
                        {
                            header.insert({stored_column.type->createColumn(), stored_column.type, stored_column.name});
                            break;
                        }
                    }
                }

                MemorySourceDefaults::Evaluation new_evaluation;
                bool is_stateful = false;
                auto dag = DB::evaluateMissingDefaults(header, required, cache.storage_snapshot->metadata->getColumns(), cache.context);
                if (dag)
                {
                    /// An input is a stored column or a subcolumn of one: the actions read `t.a` for a default naming it.
                    const auto options = GetColumnsOptions(GetColumnsOptions::AllPhysical).withSubcolumns();
                    for (const auto & input : dag->getRequiredColumns())
                    {
                        if (block.has(input.name))
                            continue;

                        auto input_in_storage = cache.storage_snapshot->tryGetColumn(options, input.name);
                        if (!input_in_storage)
                            throw Exception(ErrorCodes::LOGICAL_ERROR, "Input '{}' of the default expressions is not a column of the table", input.name);
                        new_evaluation.stored_inputs_to_read.push_back(std::move(*input_in_storage));
                    }

                    is_stateful = dag->hasStatefulFunctions();
                    dag->addMaterializingOutputActions(/*materialize_sparse=*/ false);
                    new_evaluation.actions = std::make_shared<ExpressionActions>(std::move(*dag), ExpressionActionsSettings(cache.context));
                }

                /// A stateful function keeps its state in the actions, so they are built for every block.
                if (is_stateful)
                {
                    uncached_evaluation = std::move(new_evaluation);
                    evaluation = &uncached_evaluation;
                }
                else
                {
                    evaluation = &cache.evaluations.emplace(std::move(key), std::move(new_evaluation)).first->second;
                }
            }
        }

        const auto & inputs_to_read = evaluation->stored_inputs_to_read;
        Columns inputs = readStoredColumns(src, inputs_to_read, mask, num_rows, read_bytes);
        /// An input the block lacks is a subcolumn of a requested column filled with the default value of its type.
        fillMissingColumns(inputs, num_rows, inputs_to_read, inputs_to_read, {}, nullptr);
        auto input_it = inputs_to_read.begin();
        for (const auto & input : inputs)
        {
            block.insert({input, input_it->type, input_it->name});
            ++input_it;
        }

        if (evaluation->actions)
        {
            size_t rows = num_rows;
            evaluation->actions->execute(block, rows);
        }

        it = columns.begin();
        for (size_t i = 0; i < result.size(); ++i, ++it)
        {
            if (!is_missing[i])
                continue;

            if (block.has(it->name))
                result[i] = block.getByName(it->name).column;
            else if (it->isSubcolumn())
                result[i] = tryGetSubcolumnFromBlock(block, it->getTypeInStorage(), *it);
            else
                result[i] = block.getByName(it->getNameInStorage()).column;

            read_bytes += result[i]->byteSize();
        }
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
    /// The block is assembled with an entry for every requested column, in the requested order,
    /// because the layout `ExpressionActions::execute` produces (outputs first, then the input
    /// columns it did not consume, in their block order) depends on which named entries are
    /// present - and it must reproduce the output header, which was built by running the same
    /// actions on the full sample block in `SourceStepWithFilter::applyPrewhereActions`.
    /// Entries for the columns no step consumes are created with a null column and are read
    /// from the stored block at the end, only when some rows pass and only for those rows.
    std::optional<Chunk> generateFiltered(const Block & src)
    {
        const size_t num_src_rows = src.rows();

        /// The size of the columns materialized from this block, accumulated as they are read.
        size_t num_read_bytes = 0;

        Block block;
        {
            Columns filter_columns = readColumns(src, filter->filter_input_columns, 1, nullptr, num_src_rows, num_read_bytes);

            auto filter_column_it = filter_columns.begin();
            auto filter_input_it = filter->filter_input_columns.begin();
            for (const auto & name_and_type : physical_columns)
            {
                if (filter_input_it != filter->filter_input_columns.end() && filter_input_it->name == name_and_type.name)
                {
                    block.insert({*filter_column_it, name_and_type.type, name_and_type.name});
                    ++filter_column_it;
                    ++filter_input_it;
                }
                else
                {
                    block.insert({nullptr, name_and_type.type, name_and_type.name});
                }
            }
        }

        size_t num_rows = num_src_rows;
        const bool has_deferred_columns = !filter->deferred_columns.empty();

        /// Mask over the stored block's rows combining all steps, for cutting the deferred
        /// columns at the end. Empty while no step has filtered anything.
        IColumn::Filter combined_mask;

        for (const auto & step : filter->steps)
        {
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
                        if (elem.column)
                            elem.column = filter_description.filter(*elem.column, num_passed_rows);
                }

                if (has_deferred_columns)
                {
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

        if (has_deferred_columns)
        {
            Columns deferred_columns = readColumns(
                src, filter->deferred_columns, 2, combined_mask.empty() ? nullptr : &combined_mask, num_rows, num_read_bytes);

            auto deferred_it = deferred_columns.begin();
            for (auto & elem : block)
            {
                if (elem.column)
                    continue;
                chassert(deferred_it != deferred_columns.end());
                elem.column = std::move(*deferred_it);
                ++deferred_it;
            }
            chassert(deferred_it == deferred_columns.end());
        }

        progress(num_src_rows, num_read_bytes);
        return Chunk(block.getColumns(), num_rows);
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
    MemorySourceDefaultsPtr defaults;
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

MemorySourceFilterPtr ReadFromMemoryStorageStep::makeSourceFilter(const NamesAndTypesList & physical_columns) const
{
    if (!query_info.row_level_filter && !query_info.prewhere_info)
        return nullptr;

    auto result = std::make_shared<MemorySourceFilter>();
    ExpressionActionsSettings actions_settings(context);

    /// The row-level security filter runs first, so PREWHERE expressions are never evaluated
    /// on the rows the policy hides.
    if (query_info.row_level_filter)
    {
        const auto & row_level_filter = *query_info.row_level_filter;
        result->steps.push_back({
            .actions = std::make_shared<ExpressionActions>(row_level_filter.actions.clone(), actions_settings),
            .filter_column_name = row_level_filter.column_name,
            .remove_filter_column = row_level_filter.do_remove_column,
        });
    }

    if (query_info.prewhere_info)
    {
        const auto & prewhere_info = *query_info.prewhere_info;
        result->steps.push_back({
            .actions = std::make_shared<ExpressionActions>(prewhere_info.prewhere_actions.clone(), actions_settings),
            .filter_column_name = prewhere_info.prewhere_column_name,
            .remove_filter_column = prewhere_info.remove_prewhere_column,
        });
    }

    NameSet filter_input_names;
    for (const auto & step : result->steps)
        for (const auto & required_column_name : step.actions->getRequiredColumns())
            filter_input_names.insert(required_column_name);

    for (const auto & name_and_type : physical_columns)
    {
        if (filter_input_names.contains(name_and_type.name))
            result->filter_input_columns.push_back(name_and_type);
        else
            result->deferred_columns.push_back(name_and_type);
    }

    return result;
}

MemorySourceDefaultsPtr ReadFromMemoryStorageStep::makeSourceDefaults(const NamesAndTypesList & physical_columns) const
{
    bool has_default = false;
    for (const auto & column : physical_columns)
    {
        if (storage_snapshot->getDefault(column.name) || storage_snapshot->getDefault(column.getNameInStorage()))
        {
            has_default = true;
            break;
        }
    }

    if (!has_default)
        return nullptr;

    auto result = std::make_shared<MemorySourceDefaults>();
    result->storage_snapshot = storage_snapshot;

    const auto options = GetColumnsOptions(GetColumnsOptions::AllPhysical).withSubcolumns();
    NameSet stored_input_names;
    for (const auto & column : storage_snapshot->metadata->getColumns())
    {
        if (!column.default_desc.expression)
            continue;

        IdentifierNameSet identifiers;
        column.default_desc.expression->collectIdentifierNames(identifiers);
        for (const auto & identifier : identifiers)
        {
            auto stored_column = storage_snapshot->tryGetColumn(options, identifier);
            if (stored_column && stored_input_names.emplace(stored_column->getNameInStorage()).second)
                result->stored_inputs.emplace_back(stored_column->getNameInStorage(), stored_column->getTypeInStorage());
        }
    }

    auto default_context = Context::createCopy(context->getGlobalContext());
    enableAllExperimentalSettings(default_context);
    result->context = std::move(default_context);

    return result;
}

Pipe ReadFromMemoryStorageStep::makePipe()
{
    storage_snapshot->check(columns_to_read);

    auto [physical_column_names, virtual_column_names] = VirtualColumnUtils::splitPhysicalAndVirtualColumnNames(columns_to_read, storage_snapshot);
    auto physical_columns = storage_snapshot->getColumnsByNames(GetColumnsOptions(GetColumnsOptions::All).withSubcolumns(), physical_column_names);
    auto virtual_columns = storage_snapshot->getColumnsByNames(GetColumnsOptions(GetColumnsOptions::All).withVirtuals(VirtualsKind::All, VirtualsMaterializationPlace::Reader), virtual_column_names);

    auto source_filter = makeSourceFilter(physical_columns);
    auto source_defaults = makeSourceDefaults(physical_columns);

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
            output_header,
            source_defaults));
    }

    size_t size = current_data->size();
    num_streams = std::min(num_streams, size);
    Pipes pipes;

    auto parallel_execution_index = std::make_shared<std::atomic<size_t>>(0);

    for (size_t stream = 0; stream < num_streams; ++stream)
    {
        auto source = std::make_shared<MemorySource>(
            physical_columns, virtual_columns, current_data, parallel_execution_index, nullptr, nullptr, source_filter, output_header,
            source_defaults);
        if (stream == 0)
            source->addTotalRowsApprox(snapshot_data.rows);
        pipes.emplace_back(std::move(source));
    }
    return Pipe::unitePipes(std::move(pipes));
}

}
