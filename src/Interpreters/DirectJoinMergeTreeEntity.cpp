#include <Interpreters/DirectJoinMergeTreeEntity.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnSet.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeSet.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/Set.h>
#include <Interpreters/PreparedSets.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/IStorage.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageSnapshot.h>
#include <Common/logger_useful.h>
#include <Common/HashTable/HashMap.h>
#include <Common/ColumnsHashing.h>
#include <Common/Arena.h>
#include <Planner/Utils.h>
#include <Common/AllocatorWithMemoryTracking.h>
#include <Common/ArenaAllocator.h>
#include <Functions/CastOverloadResolver.h>


namespace ProfileEvents
{
    extern const Event JoinBuildTableRowCount;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

DirectJoinMergeTreeEntity::DirectJoinMergeTreeEntity(
        QueryPlan && lookup_plan_,
        ActionsDAG filter_dag_,
        ContextPtr context_)
    : lookup_plan(std::move(lookup_plan_))
    , filter_dag(std::move(filter_dag_))
    , context(context_)
    , plan_optimization_settings(context)
    , pipeline_build_settings(context)
    , log(getLogger("DirectJoinMergeTreeEntity"))
{
    if (filter_dag.getOutputs().size() != 1)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Direct join with merge tree supports only single-column key");
}

Names DirectJoinMergeTreeEntity::getPrimaryKey() const
{
    return filter_dag.getNames();
}

Block DirectJoinMergeTreeEntity::getSampleBlock(const Names & required_columns) const
{
    const auto & sample_block = lookup_plan.getCurrentHeader();

    if (required_columns.empty())
        return *sample_block;

    Block result_block;
    for (const auto & column_name : required_columns)
        result_block.insert(sample_block->getByName(column_name));

    return result_block;
}

static std::unique_ptr<FilterStep> buildFilterStepWithIn(const ColumnWithTypeAndName & key_column, ActionsDAG filter_dag, const SharedHeader & source_header)
{
    if (filter_dag.getOutputs().size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Filter DAG for DirectJoinMergeTreeEntity must have single output, got DAG: {}", filter_dag.dumpDAG());

    const auto * key_node = filter_dag.getOutputs().front();

    DataTypePtr set_type = std::make_shared<DataTypeSet>();
    auto future_set = std::make_shared<FutureSetFromTuple>(CityHash_v1_0_2::uint128{}, ASTPtr{}, ColumnsWithTypeAndName{key_column}, false, SizeLimits{});

    auto set_column = ColumnSet::create(1, future_set);
    const auto * set_node = &filter_dag.addColumn(ColumnWithTypeAndName(std::move(set_column), set_type, "__set"));

    auto in_function = FunctionFactory::instance().get("in", nullptr);
    const auto * filter_node = &filter_dag.addFunction(in_function, {key_node, set_node}, {});

    auto actions_for_filter = ActionsDAG::createActionsForConjunction({filter_node}, source_header->getColumnsWithTypeAndName());
    if (!actions_for_filter)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to create actions for filter push down in DirectJoinMergeTreeEntity");

    auto filter_column_name = actions_for_filter->dag.getOutputs().at(actions_for_filter->filter_pos)->result_name;

    auto filter_step = std::make_unique<FilterStep>(source_header, std::move(actions_for_filter->dag), filter_column_name, actions_for_filter->remove_filter);
    filter_step->setStepDescription("Filter by join keys");
    return filter_step;
}

Chunk DirectJoinMergeTreeEntity::executePlan(QueryPlan & plan) const
{
    plan.optimize(plan_optimization_settings);

    auto pipeline_builder = plan.buildQueryPipeline(plan_optimization_settings, pipeline_build_settings);
    auto pipeline = QueryPipelineBuilder::getPipeline(std::move(*pipeline_builder));

    pipeline.setProgressCallback([](const auto & progress) { ProfileEvents::increment(ProfileEvents::JoinBuildTableRowCount, progress.read_rows); });

    PullingPipelineExecutor executor(pipeline);

    Chunk result_chunk;
    Block result_block;

    while (executor.pull(result_block))
    {
        if (!result_chunk)
        {
            result_chunk = Chunk(result_block.getColumns(), result_block.rows());
        }
        else
        {
            size_t total_rows = result_chunk.getNumRows() + result_block.rows();
            auto columns = result_chunk.detachColumns();
            const auto & new_columns = result_block.getColumns();

            for (size_t i = 0; i < columns.size(); ++i)
            {
                auto mutable_col = IColumn::mutate(std::move(columns[i]));
                mutable_col->insertRangeFrom(*new_columns[i], 0, new_columns[i]->size());
                columns[i] = std::move(mutable_col);
            }

            result_chunk.setColumns(std::move(columns), total_rows);
        }
    }

    return result_chunk;
}

Chunk DirectJoinMergeTreeEntity::getByKeys(
    const ColumnsWithTypeAndName & keys,
    const Names & required_columns,
    PaddedPODArray<UInt8> & out_null_map,
    IColumn::Offsets & out_offsets) const
{
    if (keys.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "DirectJoinMergeTreeEntity only supports single-column keys, got: [{}]", Block(keys).dumpStructure());

    if (!lookup_plan.isInitialized())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Query plan for direct join is not initialized");

    auto sample_block = getSampleBlock(required_columns);

    const size_t num_keys = keys[0].column->size();
    if (num_keys == 0)
        return Chunk(sample_block.cloneEmptyColumns(), 0);

    if (filter_dag.getOutputs().size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Filter DAG must have single output");

    QueryPlan plan = lookup_plan.clone();
    plan.addStep(buildFilterStepWithIn(keys[0], filter_dag.clone(), plan.getCurrentHeader()));

    const String & key_column_name = filter_dag.getOutputs().front()->result_name;
    auto plan_header = plan.getCurrentHeader();
    size_t key_column_idx = plan_header->getPositionByName(key_column_name);
    auto header_key_column = plan_header->getByPosition(key_column_idx);

    /// We need to convert key type to common type to perform comparison via hash map.
    FunctionBasePtr func_cast = nullptr;
    if (!header_key_column.type->equals(*keys[0].type))
        /// Left key type expected to be supertype of columns from left and right sides.
        /// It's checked during analysis and planning.
        /// So, here we expect that column from right side can be safely casted to that supertype.
        func_cast = createInternalCast(header_key_column, keys[0].type, CastType::accurate, {}, context);

    auto found_chunk = executePlan(plan);

    out_offsets.clear();
    if (!found_chunk)
    {
        auto result_columns = sample_block.cloneEmptyColumns();
        for (auto & col : result_columns)
            col->insertManyDefaults(num_keys);
        out_null_map.resize_fill(num_keys, 0);
        return Chunk(std::move(result_columns), num_keys);
    }

    size_t num_found_rows = found_chunk.getNumRows();

    auto found_key_column = found_chunk.getColumns()[key_column_idx];
    if (func_cast)
    {
        found_key_column = func_cast->execute(
            ColumnsWithTypeAndName{{found_key_column, header_key_column.type, ""}},
            keys[0].type, found_key_column->size(), false);
    }

    using IndexPositions = PODArray<UInt64, 4, AlignedArenaAllocator<sizeof(UInt64)>>;

    /// Build a hash map from result keys to their row indices using ColumnsHashing
    using Map = HashMap<UInt128, IndexPositions, UInt128TrivialHash>;
    using Method = ColumnsHashing::HashMethodHashed<typename Map::value_type, typename Map::mapped_type, false>;

    Map key_to_rows;
    Arena pool;

    Method method({found_key_column.get()}, {}, nullptr);
    for (size_t i = 0; i < num_found_rows; ++i)
    {
        auto emplace_result = method.emplaceKey(key_to_rows, i, pool);
        auto & row_list = emplace_result.getMapped();
        row_list.push_back(i, &pool);
    }
    /// All values inserted, can release the column
    found_key_column.reset();

    Method key_getter({keys[0].column.get()}, {}, nullptr);

    out_offsets.reserve(num_keys);
    out_null_map.reserve(num_keys);

    const auto & found_columns = found_chunk.mutateColumns();

    auto selector = ColumnUInt64::create();
    auto & selector_data = selector->getData();
    selector_data.reserve(num_keys);

    for (size_t i = 0; i < num_keys; ++i)
    {
        auto find_result = key_getter.findKey(key_to_rows, i, pool);

        if (find_result.isFound())
        {
            /// Key found, add all matching row indices
            const auto & matching_rows = find_result.getMapped();
            for (size_t row_idx : matching_rows)
            {
                selector_data.push_back(row_idx);
                out_null_map.push_back(1);
            }
        }
        else
        {
            /// Key not found: use sentinel index pointing to default values appended below
            selector_data.push_back(num_found_rows);
            out_null_map.push_back(0);
        }

        out_offsets.push_back(selector_data.size());
    }

    /// Append default values to each result column to handle not found keys.
    /// These serve as the target for the sentinel index (num_found_rows) used when keys are not found.
    for (const auto & col : found_columns)
        col->insertDefault();

    MutableColumns result_columns;
    for (auto && col : found_columns)
        result_columns.push_back(IColumn::mutate(col->index(*selector, 0)));

    return Chunk(std::move(result_columns), out_offsets[out_offsets.size() - 1]);
}

}
