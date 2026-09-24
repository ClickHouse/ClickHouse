#include <Processors/QueryPlan/Optimizations/Optimizations.h>

#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinLazyColumnsStep.h>
#include <Processors/QueryPlan/LazilyReadFromMergeTree.h>
#include <Processors/QueryPlan/LazilyReadFromObjectStorage.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/Optimizations/lazyFrontier.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/ReadFromObjectStorageStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/Transforms/LazyMaterializingTransform.h>
#include <Common/typeid_cast.h>

namespace DB::QueryPlanOptimizations
{

namespace
{

/// The name `LazyMaterializingTransform` looks the row index up by.
constexpr std::string_view global_row_index_name = "__global_row_index";

/// A value that crosses the `LIMIT` travels up the main branch under a name of its own, so that nothing
/// it passes can consume it by accident and the expression above the `LIMIT` can find it unambiguously.
constexpr std::string_view crossing_column_prefix = "__lazy_crossing_";

/// The name a value crosses the `LIMIT` under: the prefix and a number keep it unique, the original name
/// keeps `EXPLAIN` readable. A very long original name is cut, at a character boundary.
String makeCrossingName(size_t number, std::string_view original)
{
    constexpr size_t max_original_length = 64;
    if (original.size() > max_original_length)
    {
        size_t length = max_original_length;
        /// Step back over UTF-8 continuation bytes, so a character is not split in two.
        while (length > 0 && (static_cast<unsigned char>(original[length]) & 0xC0) == 0x80)
            --length;
        original = original.substr(0, length);
    }

    return fmt::format("{}{}_{}", crossing_column_prefix, number, original);
}

/// Whether a second, row-addressed read of this source is possible, which is what deferring a column of
/// it comes down to. The same conditions the other lazy materialization path applies to its one source.
bool canReadLazily(const QueryPlan::Node & source, const QueryPlanOptimizationSettings & settings)
{
    auto * step = source.step.get();

    if (auto * merge_tree = typeid_cast<ReadFromMergeTree *>(step))
    {
        /// Allow FINAL only for ReplacingMergeTree.
        if (merge_tree->isQueryWithFinal()
            && merge_tree->getMergeTreeData().merging_params.mode != MergeTreeData::MergingParams::Replacing)
            return false;

        if (merge_tree->isQueryWithSampling())
            return false;

        return !merge_tree->getMutationsSnapshot()->hasPatchParts();
    }

    if (auto * object_storage = typeid_cast<ReadFromObjectStorageStep *>(step))
        return settings.optimize_lazy_materialization_for_object_storage && object_storage->canUseLazyMaterialization();

    return false;
}

/// The row index of a MergeTree read, as the offset of the part in the read plus the offset of the row in
/// its part. The read is asked for the two virtual columns this is computed from, and the ones it did not
/// produce before are consumed here, so they do not leak into the rest of the plan.
ActionsDAG makeGlobalRowIndexDAG(ReadFromMergeTree & reading)
{
    bool added_part_starting_offset = false;
    bool added_part_offset = false;
    reading.addStartingPartOffsetAndPartOffset(added_part_starting_offset, added_part_offset);

    ActionsDAG dag;
    DataTypePtr uint64_type = std::make_shared<DataTypeUInt64>();
    const auto & part_starting_offset = dag.addInput("_part_starting_offset", uint64_type);
    const auto & part_offset = dag.addInput("_part_offset", uint64_type);

    auto plus = FunctionFactory::instance().get("plus", nullptr);
    const auto & index = dag.addFunction(plus, {&part_starting_offset, &part_offset}, {});
    dag.getOutputs().push_back(&dag.addAlias(index, String(global_row_index_name)));

    if (!added_part_starting_offset)
        dag.getOutputs().push_back(&part_starting_offset);
    if (!added_part_offset)
        dag.getOutputs().push_back(&part_offset);

    return dag;
}

/// The `Expression` and `Filter` steps between the `LIMIT` and `source`, bottom-up, when that is a chain
/// this path rebuilds step by step.
std::optional<std::vector<QueryPlan::Node *>> collectChain(QueryPlan::Node * chain_top, const QueryPlan::Node * source)
{
    std::vector<QueryPlan::Node *> chain;
    for (auto * node = chain_top; node != source; node = node->children.front())
    {
        if (node->children.size() != 1)
            return {};

        const ActionsDAG * dag = nullptr;
        if (auto * expression = typeid_cast<ExpressionStep *>(node->step.get()))
        {
            /// Such a step keeps inputs nothing reads, which the main branch below it would not have.
            if (expression->isInputRemovalPrevented())
                return {};
            dag = &expression->getExpression();
        }
        else if (auto * filter = typeid_cast<FilterStep *>(node->step.get()))
        {
            if (filter->isInputRemovalPrevented())
                return {};
            dag = &filter->getExpression();
        }
        else
            return {};

        /// A rebuilt step consumes by name what the original consumed, which a name read twice makes
        /// ambiguous.
        NameSet input_names;
        for (const auto * input : dag->getInputs())
            if (!input_names.insert(input->result_name).second)
                return {};

        chain.push_back(node);
    }

    std::ranges::reverse(chain);
    return chain;
}

/// A value that crosses the `LIMIT`, and the name it crosses under.
struct Crossing
{
    const ActionsDAG::Node * node = nullptr;
    String name;
};

/// What the main branch keeps of one step: the outputs something below the `LIMIT` needs, and each value
/// of this step that crosses the `LIMIT`, exported under its crossing name. Every input the original step
/// consumes that the new input header still has is consumed as well, so the columns that pass the step by
/// are the ones that passed it by before - besides the columns this path adds, which have names of their
/// own.
ActionsDAG rebuildStepDAG(
    const ActionsDAG & original,
    const ActionsDAG::NodeMapping & to_merged,
    const LazyFrontier & frontier,
    const std::vector<std::pair<const ActionsDAG::Node *, String>> & crossing_here,
    const Block & header)
{
    ActionsDAG::NodeRawConstPtrs kept_outputs;
    for (const auto * output : original.getOutputs())
        if (frontier.at(to_merged.at(output)).computed_below)
            kept_outputs.push_back(output);

    ActionsDAG::NodeRawConstPtrs roots = kept_outputs;
    for (const auto & [node, name] : crossing_here)
        roots.push_back(node);

    ActionsDAG::NodeMapping copies;
    auto dag = ActionsDAG::cloneSubDAG(roots, copies, /*remove_aliases=*/false);

    auto & outputs = dag.getOutputs();
    outputs.clear();
    for (const auto * output : kept_outputs)
        outputs.push_back(copies.at(output));
    for (const auto & [node, name] : crossing_here)
        outputs.push_back(&dag.addAlias(*copies.at(node), name));

    NameSet consumed;
    for (const auto * input : dag.getInputs())
        consumed.insert(input->result_name);

    for (const auto * input : original.getInputs())
        if (!consumed.contains(input->result_name) && header.has(input->result_name))
            dag.addInput(header.getByName(input->result_name));

    return dag;
}

/// Rebuilds `Limit -> [Sorting] -> chain -> source` for a subtree reading one source. Declines, without
/// touching the plan, where it cannot carry the frontier out.
///
/// Not called yet: plans reading one source stay with `optimizeLazyMaterialization2`, and this is the part
/// of the rebuild the join case is built on. It was checked against that path on its test corpus.
[[maybe_unused]] bool rebuildWithOneSource(
    QueryPlan::Node & root,
    QueryPlan & query_plan,
    QueryPlan::Node * sorting_node,
    QueryPlan::Node * chain_top,
    const MergedPlanDAG & merged,
    const LazyFrontier & frontier,
    const std::vector<size_t> & sort_key_positions)
{
    auto * source_node = merged.sources.front().plan_node;
    auto * read_from_merge_tree = typeid_cast<ReadFromMergeTree *>(source_node->step.get());
    auto * read_from_object_storage = typeid_cast<ReadFromObjectStorageStep *>(source_node->step.get());
    if (!read_from_merge_tree && !read_from_object_storage)
        return false;

    const auto chain = collectChain(chain_top, source_node);
    if (!chain)
        return false;

    auto * sorting_step = sorting_node ? typeid_cast<SortingStep *>(sorting_node->step.get()) : nullptr;

    /// The same restrictions the other path applies to reading in order and to a query without ORDER BY:
    /// without a filter there is nothing for the lazy read to save in the first case, and in the second
    /// only FINAL keeps the filter from being moved into PREWHERE, where it would do the same job.
    bool has_filter = std::ranges::any_of(*chain, [](const auto * node) { return typeid_cast<const FilterStep *>(node->step.get()); });
    if (read_from_merge_tree)
        has_filter |= read_from_merge_tree->getPrewhereInfo() || read_from_merge_tree->getRowLevelFilter();
    else
        has_filter |= read_from_object_storage->getPrewhereInfo() || read_from_object_storage->getRowLevelFilter();

    if (sorting_step && sorting_step->getType() == SortingStep::Type::FinishSorting && !has_filter)
        return false;
    if (!sorting_step && (!read_from_merge_tree || !read_from_merge_tree->isQueryWithFinal() || !has_filter))
        return false;

    const auto & outputs = merged.getOutputs();
    const auto & chain_top_header = *chain_top->step->getOutputHeader();

    /// The sort keys cross under the names the sorting finds them by, so each has to be found once.
    NameSet sort_key_names;
    for (size_t position : sort_key_positions)
    {
        const auto & name = outputs[position]->result_name;
        if (std::ranges::count_if(chain_top_header, [&](const auto & column) { return column.name == name; }) != 1)
            return false;
        sort_key_names.insert(name);
    }

    for (const auto & column : chain_top_header)
        if (column.name.starts_with(crossing_column_prefix))
            return false;
    for (const auto & column : *source_node->step->getOutputHeader())
        if (column.name.starts_with(crossing_column_prefix))
            return false;

    /// The source columns the main read keeps, and the ones the lazy read is to fetch. The expression above
    /// the `LIMIT` finds a lazily read column by its name, which must not be the name of a sort key crossing
    /// on the main side as well.
    NameSet eager_names;
    std::vector<const ActionsDAG::Node *> lazy_inputs;
    for (const auto * input : merged.sources.front().inputs)
    {
        const auto placed = frontier.at(input);
        if (placed.computed_below)
            eager_names.insert(input->result_name);
        if (placed.above == Placement::Above::LazyRead)
        {
            if (sort_key_names.contains(input->result_name))
                return false;
            lazy_inputs.push_back(input);
        }
    }

    if (read_from_merge_tree && read_from_merge_tree->isQueryWithFinal())
    {
        /// The FINAL merge needs the sorting key, the version and the is_deleted columns in the main read.
        const auto & merging_params = read_from_merge_tree->getMergeTreeData().merging_params;
        for (const auto & column : read_from_merge_tree->getStorageMetadata()->getColumnsRequiredForSortingKey())
            eager_names.insert(column);
        if (!merging_params.version_column.empty())
            eager_names.insert(merging_params.version_column);
        if (!merging_params.is_deleted_column.empty())
            eager_names.insert(merging_params.is_deleted_column);
    }

    std::vector<Crossing> crossings;
    for (const auto & [node, placed] : frontier.placement)
    {
        if (placed.above != Placement::Above::Crossing)
            continue;

        /// A sort key crosses under its own name; anything else under a name of its own.
        const bool is_sort_key = std::ranges::any_of(sort_key_positions, [&](size_t position) { return outputs[position] == node; });
        crossings.push_back({node, is_sort_key ? node->result_name : String()});
    }

    /// Past this point the plan is changed, and nothing declines any more.

    std::unique_ptr<LazilyReadFromMergeTree> merge_tree_lazy_reading;
    std::unique_ptr<LazilyReadFromObjectStorage> object_storage_lazy_reading;
    if (read_from_merge_tree)
        merge_tree_lazy_reading = read_from_merge_tree->keepOnlyRequiredColumnsAndCreateLazyReadStep(eager_names);
    else
        object_storage_lazy_reading = read_from_object_storage->keepOnlyRequiredColumnsAndCreateLazyReadStep(eager_names);

    /// Nothing to leave out, which also means nothing has changed yet.
    if (!merge_tree_lazy_reading && !object_storage_lazy_reading)
        return false;

    const auto & lazy_header = merge_tree_lazy_reading ? *merge_tree_lazy_reading->getOutputHeader() : *object_storage_lazy_reading->getOutputHeader();

    /// A read keeps more than it is asked to - PREWHERE and row policy inputs, virtual columns, columns an
    /// object storage `DEFAULT` expression reads. A column meant for the lazy read that the main read kept
    /// is read there already, so it crosses instead.
    for (const auto * input : lazy_inputs)
        if (!lazy_header.has(input->result_name))
            crossings.push_back({input, String()});

    size_t next_crossing_name = 0;
    for (auto & crossing : crossings)
        if (crossing.name.empty())
            crossing.name = makeCrossingName(next_crossing_name++, crossing.node->result_name);

    /// Each crossing value is exported by the step that computes it; a source column by an expression
    /// right above the read.
    std::unordered_map<const QueryPlan::Node *, std::vector<std::pair<const ActionsDAG::Node *, String>>> crossing_by_step;
    ActionsDAG::NodeRawConstPtrs crossing_source_columns;
    std::vector<String> crossing_source_names;
    for (const auto & crossing : crossings)
    {
        if (sort_key_names.contains(crossing.name))
            continue;

        const auto & origin = merged.getOrigin(crossing.node);
        if (origin.step_node == nullptr)
        {
            crossing_source_columns.push_back(crossing.node);
            crossing_source_names.push_back(crossing.name);
        }
        else
            crossing_by_step[origin.plan_node].emplace_back(origin.step_node, crossing.name);
    }

    QueryPlan main_plan;
    main_plan.addStep(std::move(source_node->step));

    {
        ActionsDAG above_read = read_from_merge_tree ? makeGlobalRowIndexDAG(*read_from_merge_tree) : ActionsDAG();
        const auto & read_header = *main_plan.getCurrentHeader();
        for (size_t i = 0; i < crossing_source_columns.size(); ++i)
        {
            /// The row index reads `_part_offset` and `_part_starting_offset` already, and a query can
            /// select those as well, in which case they cross like any other column read eagerly.
            const auto & name = crossing_source_columns[i]->result_name;
            const auto & inputs = above_read.getInputs();
            const auto existing = std::ranges::find_if(inputs, [&](const auto * input) { return input->result_name == name; });

            const ActionsDAG::Node * input = nullptr;
            if (existing != inputs.end())
            {
                input = *existing;
            }
            else
            {
                input = &above_read.addInput(read_header.getByName(name));
                above_read.getOutputs().push_back(input);
            }

            above_read.getOutputs().push_back(&above_read.addAlias(*input, crossing_source_names[i]));
        }

        if (!above_read.getOutputs().empty())
        {
            auto step = std::make_unique<ExpressionStep>(main_plan.getCurrentHeader(), std::move(above_read));
            step->setStepDescription("Row index and columns crossing the LIMIT");
            main_plan.addStep(std::move(step));
        }
    }

    for (auto * node : *chain)
    {
        static const std::vector<std::pair<const ActionsDAG::Node *, String>> nothing;
        const auto crossing_it = crossing_by_step.find(node);
        const auto & crossing_here = crossing_it == crossing_by_step.end() ? nothing : crossing_it->second;
        const auto & to_merged = merged.step_mappings.at(node);
        const auto & header = *main_plan.getCurrentHeader();

        QueryPlanStepPtr step;
        if (auto * expression = typeid_cast<ExpressionStep *>(node->step.get()))
        {
            auto dag = rebuildStepDAG(expression->getExpression(), to_merged, frontier, crossing_here, header);
            step = std::make_unique<ExpressionStep>(main_plan.getCurrentHeader(), std::move(dag));
        }
        else
        {
            auto * filter = typeid_cast<FilterStep *>(node->step.get());
            auto dag = rebuildStepDAG(filter->getExpression(), to_merged, frontier, crossing_here, header);
            step = std::make_unique<FilterStep>(
                main_plan.getCurrentHeader(), std::move(dag), filter->getFilterColumnName(), filter->removesFilterColumn());
        }

        step->setStepDescription(*node->step);
        main_plan.addStep(std::move(step));
    }

    /// Hand over exactly the sort keys, the crossing values and the row index; the sort then carries no
    /// more than that.
    {
        const auto & header = *main_plan.getCurrentHeader();
        ActionsDAG projection(header.getColumnsWithTypeAndName());
        const auto inputs = projection.getInputs();
        auto find_input = [&](const String & name) { return inputs[header.getPositionByName(name)]; };

        auto & projection_outputs = projection.getOutputs();
        projection_outputs.clear();
        for (size_t position : sort_key_positions)
            projection_outputs.push_back(find_input(outputs[position]->result_name));
        for (const auto & crossing : crossings)
            if (!sort_key_names.contains(crossing.name))
                projection_outputs.push_back(find_input(crossing.name));
        projection_outputs.push_back(find_input(String(global_row_index_name)));

        auto step = std::make_unique<ExpressionStep>(main_plan.getCurrentHeader(), std::move(projection));
        step->setStepDescription("Columns crossing the LIMIT");
        main_plan.addStep(std::move(step));
    }

    if (sorting_step)
    {
        auto new_sorting_step = std::move(sorting_node->step);
        new_sorting_step->updateInputHeader(main_plan.getCurrentHeader());
        main_plan.addStep(std::move(new_sorting_step));
    }

    /// `LimitStep::updateOutputHeader` mirrors its input header, so capture the header the replacement has
    /// to produce before the limit gets the new one.
    auto expected_header = root.step->getOutputHeader();
    root.step->updateInputHeader(main_plan.getCurrentHeader());
    main_plan.addStep(std::move(root.step));

    QueryPlan lazy_plan;
    ILazyMaterializingRowsPtr lazy_materializing_rows;
    if (merge_tree_lazy_reading)
    {
        /// The lazy read fetches exactly the rows the main read selected, addressed by their row index.
        /// It must not apply the vector search rescoring filter again: that one belongs to the main read,
        /// and applying it against the candidates of the vector index can drop a requested row.
        auto lazy_parts = read_from_merge_tree->getParts();
        for (auto & part : lazy_parts)
            part.read_hints.use_vector_search_result_filter = false;

        auto rows = std::make_shared<LazyMaterializingRows>(std::move(lazy_parts));
        merge_tree_lazy_reading->setLazyMaterializingRows(rows);
        lazy_materializing_rows = std::move(rows);
        lazy_plan.addStep(std::move(merge_tree_lazy_reading));
    }
    else
    {
        auto rows = std::make_shared<ObjectStorageLazyMaterializingRows>(read_from_object_storage->getLazyRowIndexRegistry());
        object_storage_lazy_reading->setLazyMaterializingRows(rows);
        lazy_materializing_rows = std::move(rows);
        lazy_plan.addStep(std::move(object_storage_lazy_reading));
    }

    auto join_lazy_columns = std::make_unique<JoinLazyColumnsStep>(
        main_plan.getCurrentHeader(), lazy_plan.getCurrentHeader(), lazy_materializing_rows);

    QueryPlan result_plan;
    std::vector<QueryPlanPtr> plans;
    plans.emplace_back(std::make_unique<QueryPlan>(std::move(main_plan)));
    plans.emplace_back(std::make_unique<QueryPlan>(std::move(lazy_plan)));
    result_plan.unitePlans(std::move(join_lazy_columns), std::move(plans));

    /// Above the `LIMIT`, compute what the chain produced from what crossed and what the lazy read
    /// returned. Every other column is consumed, so the header comes out exactly as it was.
    {
        const auto & header = *result_plan.getCurrentHeader();

        ActionsDAG names;
        std::unordered_map<const ActionsDAG::Node *, const ActionsDAG::Node *> new_inputs;
        for (const auto & crossing : crossings)
            new_inputs.emplace(crossing.node, &names.addInput(crossing.name, crossing.node->result_type));
        for (const auto * input : lazy_inputs)
            if (lazy_header.has(input->result_name))
                new_inputs.emplace(input, &names.addInput(input->result_name, lazy_header.getByName(input->result_name).type));

        auto above = ActionsDAG::foldActionsByProjection(new_inputs, outputs);

        NameSet consumed;
        for (const auto * input : above.getInputs())
            consumed.insert(input->result_name);
        for (const auto & column : header)
            if (!consumed.contains(column.name))
                above.addInput(column);

        auto step = std::make_unique<ExpressionStep>(result_plan.getCurrentHeader(), std::move(above));
        step->setStepDescription("Computed after the LIMIT");
        result_plan.addStep(std::move(step));
    }

    query_plan.replaceNodeWithPlan(&root, std::move(result_plan), std::move(expected_header));
    return true;
}

}

bool optimizeLazyMaterialization3(
    QueryPlan::Node & root, QueryPlan & /*query_plan*/, QueryPlan::Nodes & /*nodes*/,
    const QueryPlanOptimizationSettings & settings, size_t max_limit_for_lazy_materialization)
{
    if (root.children.size() != 1)
        return false;

    auto * limit_step = typeid_cast<LimitStep *>(root.step.get());
    if (!limit_step)
        return false;

    /// It is not known how many rows LIMIT WITH TIES reads, so there is no telling what a second read
    /// would have to fetch.
    if (limit_step->withTies())
        return false;

    const auto limit = limit_step->getLimit();
    if (limit == 0 || (max_limit_for_lazy_materialization != 0 && limit > max_limit_for_lazy_materialization))
        return false;

    /// The chain of steps down to the sources starts below the sorting, or below the limit when the
    /// query has no ORDER BY.
    auto * chain_top = root.children.front();
    [[maybe_unused]] QueryPlan::Node * sorting_node = nullptr;
    SortDescription sort_description;
    if (auto * sorting_step = typeid_cast<SortingStep *>(chain_top->step.get()))
    {
        if (sorting_step->getType() != SortingStep::Type::Full && sorting_step->getType() != SortingStep::Type::FinishSorting)
            return false;

        sorting_node = chain_top;
        sort_description = sorting_step->getSortDescription();
        chain_top = chain_top->children.front();
    }

    const auto merged = buildMergedPlanDAG(*chain_top);

    std::vector<bool> lazy_sources(merged.sources.size(), false);
    bool has_lazy_source = false;
    for (size_t source = 0; source < merged.sources.size(); ++source)
    {
        lazy_sources[source] = canReadLazily(*merged.sources[source].plan_node, settings);
        has_lazy_source |= lazy_sources[source];
    }

    if (!has_lazy_source)
        return false;

    /// The sort keys are needed below the LIMIT whatever else is deferred.
    const auto & outputs = merged.getOutputs();
    std::vector<size_t> sort_key_positions;
    for (const auto & description : sort_description)
    {
        const auto it = std::ranges::find_if(outputs, [&](const auto * output) { return output->result_name == description.column_name; });
        if (it == outputs.end())
            return false;
        sort_key_positions.push_back(it - outputs.begin());
    }
    std::ranges::sort(sort_key_positions);
    sort_key_positions.erase(std::unique(sort_key_positions.begin(), sort_key_positions.end()), sort_key_positions.end());

    const auto frontier = chooseLazyFrontier(merged, sort_key_positions, lazy_sources);
    if (!frontier.defersAnything())
        return false;

    /// A subtree reading one source is what `optimizeLazyMaterialization2` handles, and it keeps doing so.
    /// Plans with joins are not rebuilt yet.
    return false;
}

}
