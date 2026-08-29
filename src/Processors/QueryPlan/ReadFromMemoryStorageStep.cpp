#include <Processors/QueryPlan/ReadFromMemoryStorageStep.h>

#include <Analyzer/TableNode.h>

#include <Common/Exception.h>
#include <Common/typeid_cast.h>

#include <Columns/ColumnConst.h>
#include <Columns/FilterDescription.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/getColumnFromBlock.h>
#include <Interpreters/inplaceBlockConversions.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/MaterializedCTE.h>
#include <Storages/StorageSnapshot.h>
#include <Storages/StorageMemory.h>
#include <Storages/VirtualColumnUtils.h>

#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/ISource.h>
#include <Processors/Sources/NullSource.h>

#include <atomic>
#include <functional>
#include <memory>

#include <fmt/ranges.h>

namespace DB
{

namespace ErrorCodes
{

extern const int ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER;
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
        bool remove_filter_column = false;
        /// Mirrors the header-side constant replacement in `SourceStepWithFilter::applyPrewhereActions`:
        /// set for a PREWHERE step that filters but keeps its filter column.
        bool replace_filter_to_constant = false;
    };

    std::vector<Step> steps;

    /// The requested physical columns, partitioned by whether some step consumes them.
    /// Both lists preserve the requested order.
    NamesAndTypesList filter_input_columns;
    NamesAndTypesList deferred_columns;
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
                /// without reading the rest of its columns.
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

        Block block;
        {
            Columns filter_columns;
            filter_columns.reserve(filter->filter_input_columns.size());
            for (const auto & name_and_type : filter->filter_input_columns)
                filter_columns.emplace_back(readColumn(src, name_and_type));

            fillMissingColumns(filter_columns, num_src_rows, filter->filter_input_columns, filter->filter_input_columns, {}, nullptr);

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
                return std::nullopt;

            if (!constant_filter.always_true)
            {
                FilterDescription filter_description(*filter_column);
                const size_t num_passed_rows = filter_description.countBytesInFilter();
                if (num_passed_rows == 0)
                    return std::nullopt;

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

            /// Mirror `SourceStepWithFilter::applyPrewhereActions`, which shaped the output header.
            if (step.remove_filter_column)
                block.erase(filter_column_position);
            else if (step.replace_filter_to_constant)
                block.getByPosition(filter_column_position).column
                    = makeConstantFilterColumn(block.getByPosition(filter_column_position).type, num_rows);
        }

        if (has_deferred_columns)
        {
            Columns deferred_columns;
            deferred_columns.reserve(filter->deferred_columns.size());
            for (const auto & name_and_type : filter->deferred_columns)
                deferred_columns.emplace_back(readColumn(src, name_and_type));

            fillMissingColumns(deferred_columns, num_src_rows, filter->deferred_columns, filter->deferred_columns, {}, nullptr);

            auto deferred_it = deferred_columns.begin();
            for (auto & elem : block)
            {
                if (elem.column)
                    continue;
                chassert(deferred_it != deferred_columns.end());
                if (combined_mask.empty())
                    elem.column = std::move(*deferred_it);
                else
                    elem.column = (*deferred_it)->filter(combined_mask, num_rows);
                ++deferred_it;
            }
            chassert(deferred_it == deferred_columns.end());
        }

        return Chunk(block.getColumns(), num_rows);
    }

    static ColumnPtr makeConstantFilterColumn(const DataTypePtr & type, size_t num_rows)
    {
        WhichDataType which(removeNullable(recursiveRemoveLowCardinality(type)));
        if (which.isNativeInt() || which.isNativeUInt())
            return type->createColumnConst(num_rows, 1u)->convertToFullColumnIfConst();
        if (which.isFloat())
            return type->createColumnConst(num_rows, 1.0f)->convertToFullColumnIfConst();
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER, "Illegal type {} of column for filter", type->getName());
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
            .replace_filter_to_constant = false,
        });
    }

    if (query_info.prewhere_info)
    {
        const auto & prewhere_info = *query_info.prewhere_info;
        result->steps.push_back({
            .actions = std::make_shared<ExpressionActions>(prewhere_info.prewhere_actions.clone(), actions_settings),
            .filter_column_name = prewhere_info.prewhere_column_name,
            .remove_filter_column = prewhere_info.remove_prewhere_column,
            .replace_filter_to_constant = !prewhere_info.remove_prewhere_column && prewhere_info.need_filter,
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
