#include <Processors/QueryPlan/MergeTreeFinalMerge.h>
#include <Common/VectorWithMemoryTracking.h>

#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypeCustomSimpleAggregateFunction.h>
#include <DataTypes/DataTypeArray.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Processors/Merges/AggregatingSortedTransform.h>
#include <Processors/Merges/CoalescingSortedTransform.h>
#include <Processors/Merges/CollapsingSortedTransform.h>
#include <Processors/Merges/GraphiteRollupSortedTransform.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/Merges/ReplacingSortedTransform.h>
#include <Processors/Merges/SummingSortedTransform.h>
#include <Processors/Merges/VersionedCollapsingTransform.h>
#include <Processors/QueryPlan/PartsSplitter.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/FilterTransform.h>
#include <Processors/Transforms/SelectByIndicesTransform.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <ctime>
#include <numeric>

namespace DB
{

namespace Setting
{
    extern const SettingsBool compile_sort_description;
    extern const SettingsUInt64 min_count_to_compile_sort_description;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

String getSummingMapName(const NameAndTypePair & column, bool allow_tuple_element_aggregation)
{
    if (!typeid_cast<const DataTypeArray *>(column.type.get())
        || dynamic_cast<const DataTypeCustomSimpleAggregateFunction *>(column.type->getCustomName()))
        return {};

    String map_name = allow_tuple_element_aggregation ? Nested::splitName(column.name, /*reverse=*/true).first
                                                      : Nested::extractTableName(column.name);
    if (map_name == column.name || !endsWith(map_name, "Map"))
        return {};
    return map_name;
}

namespace
{

/// The positions of the columns of `header` with the columns of each `...Map` structure that the merge of `SummingMergeTree`
/// sums put in the order of the table, in the places that the structure takes: the merge takes the first column of a
/// structure as its key. Empty if they are in that order already.
std::vector<size_t> getSummingMapColumnsPermutation(const Block & header, const StorageMetadataPtr & metadata_snapshot, bool allow_tuple_element_aggregation)
{
    std::unordered_map<String, size_t> table_positions;
    for (const auto & column : metadata_snapshot->getColumns().getAllPhysical())
        table_positions.emplace(column.name, table_positions.size());

    std::unordered_map<String, std::vector<size_t>> map_positions;
    for (size_t i = 0; i < header.columns(); ++i)
    {
        const auto & column = header.getByPosition(i);
        if (auto map_name = getSummingMapName({column.name, column.type}, allow_tuple_element_aggregation); !map_name.empty())
            map_positions[map_name].push_back(i);
    }

    std::vector<size_t> permutation(header.columns());
    std::iota(permutation.begin(), permutation.end(), 0);
    for (const auto & [_, positions] : map_positions)
    {
        auto in_table_order = positions;
        std::ranges::sort(in_table_order, {}, [&](size_t position)
        {
            auto it = table_positions.find(header.getByPosition(position).name);
            return it == table_positions.end() ? table_positions.size() : it->second;
        });
        for (size_t i = 0; i < positions.size(); ++i)
            permutation[positions[i]] = in_table_order[i];
    }

    if (std::ranges::is_sorted(permutation))
        return {};
    return permutation;
}

/// Puts the columns of `header` at `permutation`, or back where they were with `inverse`.
ExpressionActionsPtr makePermutationActions(const Block & header, const std::vector<size_t> & permutation, bool inverse)
{
    ActionsDAG dag(header.getColumnsWithTypeAndName());
    ActionsDAG::NodeRawConstPtrs outputs(permutation.size());
    for (size_t i = 0; i < permutation.size(); ++i)
    {
        if (inverse)
            outputs[permutation[i]] = dag.getOutputs()[i];
        else
            outputs[i] = dag.getOutputs()[permutation[i]];
    }
    dag.getOutputs().swap(outputs);
    return std::make_shared<ExpressionActions>(std::move(dag));
}

}

void addMergingFinal(
    Pipe & pipe,
    const SortDescription & sort_description,
    MergeTreeData::MergingParams merging_params,
    const StorageMetadataPtr & metadata_snapshot,
    size_t max_block_size_rows,
    bool enable_vertical_final,
    bool read_in_reverse)
{
    /// A `PREWHERE` before the merge may have put the columns of a `...Map` structure in another order than a merge in the
    /// background reads them in, so the merge would take another column as the key. Restore the order for the merge only.
    std::vector<size_t> summing_map_permutation;
    if (merging_params.mode == MergeTreeData::MergingParams::Summing)
        summing_map_permutation = getSummingMapColumnsPermutation(
            pipe.getHeader(), metadata_snapshot, merging_params.allow_tuple_element_aggregation);

    if (!summing_map_permutation.empty())
    {
        auto actions = makePermutationActions(pipe.getHeader(), summing_map_permutation, /*inverse=*/false);
        pipe.addSimpleTransform([&](const SharedHeader & header_) { return std::make_shared<ExpressionTransform>(header_, actions); });
    }

    auto header = pipe.getSharedHeader();
    size_t num_outputs = pipe.numOutputPorts();

    auto now = time(nullptr);

    /// Only the Replacing merging algorithm supports reading in the reverse order
    /// (see `ReadFromMergeTree::requestReadingInOrder`).
    if (read_in_reverse && merging_params.mode != MergeTreeData::MergingParams::Replacing)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Reading in reverse order with FINAL is supported only for ReplacingMergeTree");

    auto get_merging_processor = [&]() -> MergingTransformPtr
    {
        switch (merging_params.mode)
        {
            case MergeTreeData::MergingParams::Ordinary:
                return std::make_shared<MergingSortedTransform>(header, num_outputs,
                            sort_description, max_block_size_rows, /*max_block_size_bytes=*/0, /*max_dynamic_subcolumns*/std::nullopt, SortingQueueStrategy::Batch);

            case MergeTreeData::MergingParams::Collapsing:
                return std::make_shared<CollapsingSortedTransform>(header, num_outputs,
                            sort_description, merging_params.sign_column, true, max_block_size_rows, /*max_block_size_bytes=*/0, /*max_dynamic_subcolumns*/std::nullopt);

            case MergeTreeData::MergingParams::Summing: {
                auto required_columns = metadata_snapshot->getPartitionKey().expression->getRequiredColumns();
                required_columns.append_range(metadata_snapshot->getSortingKey().expression->getRequiredColumns());
                return std::make_shared<SummingSortedTransform>(header, num_outputs,
                            sort_description, merging_params.columns_to_sum, required_columns, max_block_size_rows, /*max_block_size_bytes=*/0, /*max_dynamic_subcolumns*/std::nullopt, merging_params.allow_tuple_element_aggregation, /*remove_zero_rows=*/ false);
            }

            case MergeTreeData::MergingParams::Aggregating:
                return std::make_shared<AggregatingSortedTransform>(header, num_outputs,
                            sort_description, max_block_size_rows, /*max_block_size_bytes=*/0, /*max_dynamic_subcolumns*/std::nullopt, merging_params.allow_tuple_element_aggregation);

            case MergeTreeData::MergingParams::Replacing:
                return std::make_shared<ReplacingSortedTransform>(header, num_outputs,
                            sort_description, merging_params.is_deleted_column, merging_params.version_column, max_block_size_rows, /*max_block_size_bytes=*/0, /*max_dynamic_subcolumns*/std::nullopt, /*out_row_sources_buf_*/ nullptr, /*use_average_block_sizes*/ false, /*cleanup*/ !merging_params.is_deleted_column.empty(), enable_vertical_final, read_in_reverse);

            case MergeTreeData::MergingParams::VersionedCollapsing:
                return std::make_shared<VersionedCollapsingTransform>(header, num_outputs,
                            sort_description, merging_params.sign_column, max_block_size_rows, /*max_block_size_bytes=*/0, /*max_dynamic_subcolumns*/std::nullopt);

            case MergeTreeData::MergingParams::Graphite:
                return std::make_shared<GraphiteRollupSortedTransform>(header, num_outputs,
                            sort_description, max_block_size_rows, /*max_block_size_bytes=*/0, /*max_dynamic_subcolumns*/std::nullopt, merging_params.graphite_params, now);

            case MergeTreeData::MergingParams::Coalescing:
            {
                auto required_columns = metadata_snapshot->getPartitionKey().expression->getRequiredColumns();
                required_columns.append_range(metadata_snapshot->getSortingKey().expression->getRequiredColumns());
                return std::make_shared<CoalescingSortedTransform>(header, num_outputs,
                            sort_description, merging_params.columns_to_sum, required_columns, max_block_size_rows, /*max_block_size_bytes=*/0, /*max_dynamic_subcolumns*/std::nullopt, merging_params.allow_tuple_element_aggregation);
            }
        }
    };

    pipe.addTransform(get_merging_processor());
    if (enable_vertical_final)
        pipe.addSimpleTransform([](const SharedHeader & header_)
                                { return std::make_shared<SelectByIndicesTransform>(header_); });

    if (!summing_map_permutation.empty())
    {
        auto actions = makePermutationActions(pipe.getHeader(), summing_map_permutation, /*inverse=*/true);
        pipe.addSimpleTransform([&](const SharedHeader & header_) { return std::make_shared<ExpressionTransform>(header_, actions); });
    }
}

namespace
{

ActionsDAG createProjection(const Block & header)
{
    return ActionsDAG(header.getNamesAndTypesList());
}

std::pair<std::shared_ptr<ExpressionActions>, String> createExpressionForPositiveSign(const String & sign_column_name, const Block & header, const ContextPtr & context)
{
    ASTPtr sign_indentifier = make_intrusive<ASTIdentifier>(sign_column_name);
    ASTPtr sign_filter = makeASTOperator("equals", sign_indentifier, make_intrusive<ASTLiteral>(Field(static_cast<Int8>(1))));
    const auto & sign_column = header.getByName(sign_column_name);

    auto syntax_result = TreeRewriter(context).analyze(sign_filter, {{sign_column.name, sign_column.type}});
    auto actions = ExpressionAnalyzer(sign_filter, syntax_result, context).getActionsDAG(false);
    return {std::make_shared<ExpressionActions>(std::move(actions)), sign_filter->getColumnName()};
}

std::pair<std::shared_ptr<ExpressionActions>, String> createExpressionForIsDeleted(const String & is_deleted_column_name, const Block & header, const ContextPtr & context)
{
    ASTPtr is_deleted_identifier = make_intrusive<ASTIdentifier>(is_deleted_column_name);
    ASTPtr is_deleted_filter = makeASTFunction("equals", is_deleted_identifier, make_intrusive<ASTLiteral>(Field(static_cast<Int8>(0))));

    const auto & is_deleted_column = header.getByName(is_deleted_column_name);

    auto syntax_result = TreeRewriter(context).analyze(is_deleted_filter, {{is_deleted_column.name, is_deleted_column.type}});
    auto actions = ExpressionAnalyzer(is_deleted_filter, syntax_result, context).getActionsDAG(false);
    return {std::make_shared<ExpressionActions>(std::move(actions)), is_deleted_filter->getColumnName()};
}

}

namespace
{

/// The sort description for a FINAL merge: the table's sorting key columns, with any per-column descending flag.
SortDescription buildFinalSortDescription(const StorageMetadataPtr & metadata_snapshot, const Settings & settings)
{
    Names sort_columns = metadata_snapshot->getSortingKeyColumns();
    auto reverse_flags = metadata_snapshot->getSortingKeyReverseFlags();
    SortDescription sort_description;
    sort_description.compile_sort_description = settings[Setting::compile_sort_description];
    sort_description.min_count_to_compile_sort_description = settings[Setting::min_count_to_compile_sort_description];
    sort_description.reserve(sort_columns.size());
    for (size_t i = 0; i < sort_columns.size(); ++i)
        sort_description.emplace_back(sort_columns[i], (!reverse_flags.empty() && reverse_flags[i]) ? -1 : 1);
    return sort_description;
}

}

Pipe buildFullFinalMergePipe(
    const StorageMetadataPtr & metadata_snapshot,
    MergeTreeData::MergingParams merging_params,
    size_t max_block_size_rows,
    bool enable_vertical_final,
    ContextPtr context,
    std::optional<ActionsDAG> & out_projection,
    const std::function<Pipe()> & read_all_parts_in_order)
{
    auto sorting_expr = metadata_snapshot->getSortingKey().expression;
    auto sort_description = buildFinalSortDescription(metadata_snapshot, context->getSettingsRef());

    Pipe pipe = read_all_parts_in_order();
    if (pipe.empty())
        return pipe;

    pipe.addSimpleTransform([sorting_expr](const SharedHeader & header)
                            { return std::make_shared<ExpressionTransform>(header, sorting_expr); });
    if (!out_projection)
        out_projection = createProjection(pipe.getHeader());
    addMergingFinal(pipe, sort_description, merging_params, metadata_snapshot, max_block_size_rows, enable_vertical_final);
    return pipe;
}

Pipe buildDistributedFinalPipe(
    const VectorWithMemoryTracking<DistributedReadBucket> & lanes,
    const StorageMetadataPtr & metadata_snapshot,
    MergeTreeData::MergingParams merging_params,
    size_t max_block_size_rows,
    bool enable_vertical_final,
    ContextPtr context,
    std::optional<ActionsDAG> & out_projection,
    const DistributedFinalReadStepGetter & read_lane_in_order,
    const DistributedFinalReadStepGetter & read_non_intersecting)
{
    const auto & settings = context->getSettingsRef();
    auto sorting_expr = metadata_snapshot->getSortingKey().expression;

    const auto & primary_key = metadata_snapshot->getPrimaryKey();
    auto in_reverse_order = deriveReverseOrder(primary_key, metadata_snapshot->getSortingKey());

    SortDescription sort_description = buildFinalSortDescription(metadata_snapshot, settings);

    /// One merge pipe per intersecting lane; the non-intersecting lanes are read once. Parallelism across lanes.
    Pipes final_merge_pipes;
    RangesInDataPartsDescription non_intersecting_marks;
    for (const auto & lane : lanes)
    {
        if (!lane.needs_merge)
        {
            for (const auto & part_marks : lane.marks)
                non_intersecting_marks.push_back(part_marks);
            continue;
        }

        if (!in_reverse_order)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Distributed FINAL got primary-key-range layers for a table whose primary key cannot be range-split");

        Pipe pipe = read_lane_in_order(lane.marks);

        /// A lane may carry an empty primary-key-range layer (the layer split may produce layers with
        /// no parts, and older initiators serialize them all). The in-order getter creates one source
        /// per part, so such a lane yields an empty pipe, which has no header and must not reach the
        /// transforms below. Dropping it is safe: the merge lanes are simply united, so their positions
        /// carry no meaning.
        if (pipe.empty())
            continue;

        pipe.addSimpleTransform([sorting_expr](const SharedHeader & header)
                                { return std::make_shared<ExpressionTransform>(header, sorting_expr); });
        addLayerRangeFilterToPipe(pipe, primary_key, lane.borders, lane.index, *in_reverse_order, context);
        if (!out_projection)
            out_projection = createProjection(pipe.getHeader());
        addMergingFinal(pipe, sort_description, merging_params, metadata_snapshot, max_block_size_rows, enable_vertical_final);
        final_merge_pipes.emplace_back(std::move(pipe));
    }

    Pipes final_non_merge_pipes;
    if (!non_intersecting_marks.empty())
        final_non_merge_pipes.emplace_back(read_non_intersecting(non_intersecting_marks));

    /// An empty task (e.g. an ordinary MergeTree bucket the coordinator kept with no marks) has no pipes
    /// on either side. Return an empty pipe -- asking an empty united pipe for its header below would crash.
    if (final_merge_pipes.empty() && final_non_merge_pipes.empty())
        return {};

    if (!final_merge_pipes.empty() && !final_non_merge_pipes.empty())
    {
        out_projection = {}; /// Projection happens via the converting transform below.
        Pipes pipes;
        pipes.resize(2);
        pipes[0] = Pipe::unitePipes(std::move(final_merge_pipes));
        pipes[1] = Pipe::unitePipes(std::move(final_non_merge_pipes));
        auto conversion_action = ActionsDAG::makeConvertingActions(
            pipes[0].getHeader().getColumnsWithTypeAndName(),
            pipes[1].getHeader().getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Name,
            context);
        auto converting_expr = std::make_shared<ExpressionActions>(std::move(conversion_action));
        pipes[0].addSimpleTransform([converting_expr](const SharedHeader & header)
                                    { return std::make_shared<ExpressionTransform>(header, converting_expr); });
        return Pipe::unitePipes(std::move(pipes));
    }

    if (final_merge_pipes.empty())
    {
        Pipe pipe = Pipe::unitePipes(std::move(final_non_merge_pipes));
        if (!out_projection)
            out_projection = createProjection(pipe.getHeader());
        return pipe;
    }
    return Pipe::unitePipes(std::move(final_merge_pipes));
}

Pipe readNonIntersectingFinalWithEngineFilter(
    const MergeTreeData::MergingParams & merging_params,
    const Names & origin_column_names,
    ContextPtr context,
    const std::function<Pipe(const Names & columns)> & read)
{
    /// `Collapsing` does not expose unmatched negative-sign rows on FINAL, and `Replacing` with an is-deleted
    /// column hides deleted rows; a non-intersecting range skips the merge, so add that filter here. Other
    /// engines drop nothing on FINAL beyond the deduplication a single part already did.
    if (merging_params.mode == MergeTreeData::MergingParams::Collapsing)
    {
        auto columns_with_sign = origin_column_names;
        if (std::ranges::find(columns_with_sign, merging_params.sign_column) == columns_with_sign.end())
            columns_with_sign.push_back(merging_params.sign_column);
        Pipe pipe = read(columns_with_sign);
        auto [expression, filter_name] = createExpressionForPositiveSign(merging_params.sign_column, pipe.getHeader(), context);
        pipe.addSimpleTransform([&](const SharedHeader & header)
            { return std::make_shared<FilterTransform>(header, expression, filter_name, true); });
        return pipe;
    }
    if (!merging_params.is_deleted_column.empty())
    {
        auto columns_with_is_deleted = origin_column_names;
        if (std::ranges::find(columns_with_is_deleted, merging_params.is_deleted_column) == columns_with_is_deleted.end())
            columns_with_is_deleted.push_back(merging_params.is_deleted_column);
        Pipe pipe = read(columns_with_is_deleted);
        auto [expression, filter_name] = createExpressionForIsDeleted(merging_params.is_deleted_column, pipe.getHeader(), context);
        pipe.addSimpleTransform([&](const SharedHeader & header)
            { return std::make_shared<FilterTransform>(header, expression, filter_name, true); });
        return pipe;
    }
    return read(origin_column_names);
}

}
