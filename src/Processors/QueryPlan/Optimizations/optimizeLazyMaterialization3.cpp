#include <Processors/QueryPlan/Optimizations/Optimizations.h>

#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinLazyColumnsStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
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

#include <map>
#include <numeric>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB::QueryPlanOptimizations
{

namespace
{

/// What a MergeTree read calls the row index, and what an object storage read produces it as. The index of
/// each source crosses the `LIMIT` under this name and the number of the source.
constexpr std::string_view global_row_index_name = "__global_row_index";

/// A value that crosses the `LIMIT` travels up the main branch under a name of its own, so that nothing
/// it passes can consume it by accident and the expression above the `LIMIT` can find it unambiguously.
constexpr std::string_view crossing_column_prefix = "__lazy_crossing_";

/// A column the lazy read returns is renamed the same way: two sources can well have columns of the same
/// name, and above the `LIMIT` both are in one block.
constexpr std::string_view lazy_column_prefix = "__lazy_read_";

/// Whether a join matched a row, computed from the row index of a source the join can leave unmatched.
constexpr std::string_view mask_column_prefix = "__lazy_mask_";

/// A name of its own for a column: the prefix and a number keep it unique, the original name keeps
/// `EXPLAIN` readable. A very long original name is cut, at a character boundary.
String makeUniqueName(std::string_view prefix, size_t number, std::string_view original)
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

    return fmt::format("{}{}_{}", prefix, number, original);
}

String makeRowIndexName(size_t source)
{
    return fmt::format("{}_{}", global_row_index_name, source);
}

bool isNameOfThisPass(std::string_view name)
{
    return name.starts_with(global_row_index_name) || name.starts_with(crossing_column_prefix) || name.starts_with(lazy_column_prefix)
        || name.starts_with(mask_column_prefix);
}

String makeMaskName(size_t source)
{
    return fmt::format("{}{}", mask_column_prefix, source);
}

/// `if(mask, value, default)`: the value where the join matched the row, and the default where it stuffed
/// it, which is what the join itself stands there.
const ActionsDAG::Node & addMasked(
    ActionsDAG & dag, const ActionsDAG::Node & mask, const ActionsDAG::Node & value, const String & name, const ContextPtr & context)
{
    const auto & type = value.result_type;
    const auto & default_value = dag.addColumn(
        type->createColumnConstWithDefaultValue(0), type, fmt::format("defaultValueOfTypeName('{}')", type->getName()));

    auto if_function = FunctionFactory::instance().get("if", context);
    return dag.addFunction(if_function, {&mask, &value, &default_value}, name);
}

/// Whether masking a value keeps its type, which the result above the `LIMIT` has to.
bool canMask(const ActionsDAG::Node & node, const ContextPtr & context)
{
    ActionsDAG dag;
    const auto & mask = dag.addInput("mask", std::make_shared<DataTypeUInt8>());
    const auto & value = dag.addInput("value", node.result_type);
    return addMasked(dag, mask, value, "masked", context).result_type->equals(*node.result_type);
}

/// Whether a value is recomputed above the `LIMIT` under a mask: a function a join can stuff. What reads
/// it there reads it as the join left it, so the mask applies to the value itself.
bool isMaskedRecomputation(const MergedPlanDAG & merged, const LazyFrontier & frontier, const ActionsDAG::Node * node)
{
    return node->type == ActionsDAG::ActionType::FUNCTION && frontier.at(node).above == Placement::Above::Recomputed
        && merged.getNearestStuffing(node) != nullptr;
}

/// The expression computed above the `LIMIT`: the outputs of `merged`, from the columns `inputs` names for
/// the values that crossed or were read lazily, with every other value recomputed, and masked where
/// `isMaskedRecomputation` says so. Every column of `header` it does not read is consumed, so the result
/// has exactly the header the subtree had.
ActionsDAG buildAboveLimitDAG(
    const MergedPlanDAG & merged,
    const LazyFrontier & frontier,
    const std::unordered_map<const ActionsDAG::Node *, String> & inputs,
    const std::unordered_map<const MergedPlanDAG::Stuffing *, String> & masks,
    const Block & header,
    const ContextPtr & context)
{
    ActionsDAG dag;
    std::unordered_map<const ActionsDAG::Node *, const ActionsDAG::Node *> copies;
    std::unordered_map<String, const ActionsDAG::Node *> header_inputs;

    const auto read_column = [&](const String & name) -> const ActionsDAG::Node &
    {
        auto [it, inserted] = header_inputs.emplace(name, nullptr);
        if (inserted)
            it->second = &dag.addInput(header.getByName(name));
        return *it->second;
    };

    struct Frame
    {
        const ActionsDAG::Node * node = nullptr;
        size_t next_child = 0;
    };

    for (const auto * output : merged.getOutputs())
    {
        std::vector<Frame> stack{{output}};
        while (!stack.empty())
        {
            auto & frame = stack.back();
            const auto * node = frame.node;

            if (copies.contains(node))
            {
                stack.pop_back();
                continue;
            }

            if (const auto it = inputs.find(node); it != inputs.end())
            {
                copies.emplace(node, &read_column(it->second));
                stack.pop_back();
                continue;
            }

            if (frame.next_child < node->children.size())
            {
                stack.push_back({node->children[frame.next_child++]});
                continue;
            }

            ActionsDAG::NodeRawConstPtrs children;
            for (const auto * child : node->children)
                children.push_back(copies.at(child));

            const ActionsDAG::Node * copy = nullptr;
            switch (node->type)
            {
                case ActionsDAG::ActionType::COLUMN:
                    copy = &dag.addColumn(
                        node->column, node->result_type, node->result_name, node->is_deterministic_constant, node->is_masked_secret,
                        node->is_runtime_filter_id);
                    break;
                case ActionsDAG::ActionType::ALIAS:
                    copy = &dag.addAlias(*children.front(), node->result_name);
                    break;
                case ActionsDAG::ActionType::FUNCTION:
                    copy = &dag.addFunction(node->function_base, std::move(children), node->result_name);
                    break;
                default:
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Value {} is neither computed after the LIMIT nor available there", node->result_name);
            }

            if (isMaskedRecomputation(merged, frontier, node))
            {
                const auto & mask = read_column(masks.at(merged.getNearestStuffing(node)));
                copy = &addMasked(dag, mask, *copy, node->result_name, context);
            }

            copies.emplace(node, copy);
            stack.pop_back();
        }
    }

    auto & dag_outputs = dag.getOutputs();
    for (const auto * output : merged.getOutputs())
    {
        const auto * copy = copies.at(output);
        if (copy->result_name != output->result_name)
            copy = &dag.addAlias(*copy, output->result_name);
        dag_outputs.push_back(copy);
    }

    for (const auto & column : header)
        read_column(column.name);

    return dag;
}

/// Whether a second, row-addressed read of this source is possible, which is what deferring a column of
/// it comes down to. The conditions the other lazy materialization path applies to its one source, except
/// that FINAL is not handled here yet: the FINAL merge needs more columns in the main read than the
/// frontier knows about.
bool canReadLazily(const QueryPlan::Node & source, const QueryPlanOptimizationSettings & settings)
{
    auto * step = source.step.get();

    if (auto * merge_tree = typeid_cast<ReadFromMergeTree *>(step))
    {
        if (merge_tree->isQueryWithFinal())
            return false;

        if (merge_tree->isQueryWithSampling())
            return false;

        return !merge_tree->getMutationsSnapshot()->hasPatchParts();
    }

    if (auto * object_storage = typeid_cast<ReadFromObjectStorageStep *>(step))
        return settings.optimize_lazy_materialization_for_object_storage && object_storage->canUseLazyMaterialization();

    return false;
}

/// The row index of a read, under `index_name`. For MergeTree it is the offset of the part in the read
/// plus the offset of the row in its part: the read is asked for the two virtual columns this is computed
/// from, and the ones it did not produce before are consumed here, so they do not leak into the rest of
/// the plan. An object storage read produces the index itself. Below a join that can leave the source
/// unmatched the index is `Nullable`, so that the join stands it at NULL for the rows it stuffs.
ActionsDAG makeRowIndexDAG(ReadFromMergeTree * merge_tree, const Block & read_header, const String & index_name, bool nullable)
{
    ActionsDAG dag;
    const ActionsDAG::Node * index = nullptr;

    if (merge_tree)
    {
        bool added_part_starting_offset = false;
        bool added_part_offset = false;
        merge_tree->addStartingPartOffsetAndPartOffset(added_part_starting_offset, added_part_offset);

        DataTypePtr uint64_type = std::make_shared<DataTypeUInt64>();
        const auto & part_starting_offset = dag.addInput("_part_starting_offset", uint64_type);
        const auto & part_offset = dag.addInput("_part_offset", uint64_type);

        auto plus = FunctionFactory::instance().get("plus", nullptr);
        index = &dag.addFunction(plus, {&part_starting_offset, &part_offset}, {});

        if (!added_part_starting_offset)
            dag.getOutputs().push_back(&part_starting_offset);
        if (!added_part_offset)
            dag.getOutputs().push_back(&part_offset);
    }
    else
    {
        index = &dag.addInput(read_header.getByName(String(global_row_index_name)));
    }

    if (nullable)
    {
        auto to_nullable = FunctionFactory::instance().get("toNullable", nullptr);
        index = &dag.addFunction(to_nullable, {index}, {});
    }

    dag.getOutputs().push_back(&dag.addAlias(*index, index_name));
    return dag;
}

/// A value that crosses the `LIMIT`, and the name it crosses under.
struct Crossing
{
    const ActionsDAG::Node * node = nullptr;
    String name;
};

/// What the main branch keeps of one step: the outputs something below the `LIMIT` reads under their own
/// names, and each value of this step that crosses the `LIMIT`, exported under its crossing name. Every input the original step
/// consumes that the new input header still has is consumed as well, so the columns that pass the step by
/// are the ones that passed it by before - besides the columns this path adds, which have names of their
/// own.
ActionsDAG rebuildStepDAG(
    const ActionsDAG & original,
    const ActionsDAG::NodeMapping & to_merged,
    const NodeSet & needed_below,
    const std::vector<std::pair<const ActionsDAG::Node *, String>> & crossing_here,
    const Block & header)
{
    ActionsDAG::NodeRawConstPtrs kept_outputs;
    for (const auto * output : original.getOutputs())
        if (needed_below.contains(to_merged.at(output)))
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

bool hasUniqueNames(const Block & header)
{
    NameSet names;
    for (const auto & column : header)
        if (!names.insert(column.name).second)
            return false;
    return true;
}

/// Rebuilds `Limit -> Sorting -> tree of Expression, Filter and JoinStepLogical -> sources` so that the
/// columns the frontier defers are read for the rows the `LIMIT` returns only. The main branch is the
/// same tree, step by step - never one expression over it all: a filter decides which rows the steps
/// above it see, so computing below it what the query computes above it can throw where the query does
/// not. Each lazily read source adds its row index there, which the joins pass through, and above the
/// `LIMIT` one `JoinLazyColumnsStep` per such source reads its deferred columns by that index.
///
/// `prepare` checks everything without touching the plan and declines where the rebuild cannot carry the
/// frontier out; `apply` then changes the plan and does not decline any more.
class JoinPlanRebuild
{
public:
    /// `mask_sources` are the sources whose row index says whether a stuffing matched, for the stuffings
    /// the frontier masks values of.
    JoinPlanRebuild(
        const MergedPlanDAG & merged_,
        const LazyFrontier & frontier_,
        const std::vector<bool> & lazy_sources_,
        const std::unordered_map<const MergedPlanDAG::Stuffing *, size_t> & mask_sources_,
        ContextPtr context_)
        : merged(merged_), frontier(frontier_), lazy_sources(lazy_sources_), mask_sources(mask_sources_), context(std::move(context_))
    {
        for (size_t source = 0; source < merged.sources.size(); ++source)
            source_numbers.emplace(merged.sources[source].plan_node, source);
    }

    bool prepare(QueryPlan::Node * chain_top, const std::vector<size_t> & sort_key_positions)
    {
        size_t joins = 0;
        if (!checkSteps(chain_top, joins))
            return false;

        /// A subtree reading one source is what `optimizeLazyMaterialization2` handles.
        if (joins == 0)
            return false;

        const auto & outputs = merged.getOutputs();
        const auto & chain_top_header = *chain_top->step->getOutputHeader();

        /// The sort keys cross under the names the sorting finds them by, so each has to be found once.
        for (size_t position : sort_key_positions)
        {
            const auto & name = outputs[position]->result_name;
            if (std::ranges::count_if(chain_top_header, [&](const auto & column) { return column.name == name; }) != 1)
                return false;
            sort_key_nodes.insert(outputs[position]);
        }

        for (size_t source = 0; source < merged.sources.size(); ++source)
        {
            if (!lazy_sources[source])
                continue;

            const auto & inputs = merged.sources[source].inputs;

            LazySource lazy;
            for (const auto * input : inputs)
            {
                const auto placed = frontier.at(input);
                if (placed.computed_below)
                    lazy.eager_names.insert(input->result_name);
                if (placed.above == Placement::Above::LazyRead)
                    lazy.inputs.push_back(input);
            }

            if (lazy.inputs.empty())
                continue;

            lazy_by_source.emplace(source, std::move(lazy));
        }

        if (lazy_by_source.empty())
            return false;

        for (const auto & node : merged.getDAG().getNodes())
            if (isMaskedRecomputation(merged, frontier, &node) && !canMask(node, context))
                return false;

        for (const auto & [node, placed] : frontier.placement)
        {
            if (placed.above != Placement::Above::Crossing)
                continue;

            /// A sort key crosses under its own name, as a column the sorting needs anyway.
            if (sort_key_nodes.contains(node))
                continue;

            /// A value a join computes would have to be exported by the join itself, and a join outputs
            /// only what its expressions compute after it. Not handled yet.
            const auto & origin = merged.getOrigin(node);
            if (typeid_cast<const JoinStepLogical *>(origin.plan_node->step.get()))
                return false;

            crossings.push_back({node, String()});
        }

        /// A crossing value is computed below the `LIMIT`, but what reads it there is only its export, under
        /// its crossing name. The steps between keep a value under its own name only where a filter, a join
        /// condition, the sort order or the computation of a crossing value reads it, so that a crossing
        /// column does not travel up twice, and a join does not keep two copies of it.
        {
            ActionsDAG::NodeRawConstPtrs roots = merged.filter_nodes;
            roots.append_range(merged.join_condition_nodes);
            roots.append_range(sort_key_nodes);
            for (const auto & crossing : crossings)
                roots.append_range(crossing.node->children);
            needed_below = findReachableNodes(roots);
        }

        return std::ranges::all_of(join_nodes, [&](const auto * join_node) { return canPruneJoin(*join_node); });
    }

    void apply(QueryPlan & query_plan, QueryPlan::Nodes & nodes, QueryPlan::Node & root, QueryPlan::Node & sorting_node, QueryPlan::Node * chain_top,
        const std::vector<size_t> & sort_key_positions)
    {
        auto main_plan = rebuild(chain_top, nodes);

        /// Hand over exactly the sort keys, the crossing values and the row indexes; the sort then carries no
        /// more than that.
        const auto & outputs = merged.getOutputs();
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
                projection_outputs.push_back(find_input(crossing.name));
            for (const auto & [source, index_name] : index_names)
                projection_outputs.push_back(find_input(index_name));

            auto step = std::make_unique<ExpressionStep>(main_plan.getCurrentHeader(), std::move(projection));
            step->setStepDescription("Columns crossing the LIMIT");
            main_plan.addStep(std::move(step));
        }

        auto sorting_step = std::move(sorting_node.step);
        sorting_step->updateInputHeader(main_plan.getCurrentHeader());
        main_plan.addStep(std::move(sorting_step));

        /// `LimitStep::updateOutputHeader` mirrors its input header, so capture the header the replacement has
        /// to produce before the limit gets the new one.
        auto expected_header = root.step->getOutputHeader();
        root.step->updateInputHeader(main_plan.getCurrentHeader());
        main_plan.addStep(std::move(root.step));

        /// The masks are read off the row indexes before the lazy reads consume them.
        std::unordered_map<const MergedPlanDAG::Stuffing *, String> masks;
        if (!mask_sources.empty())
        {
            const auto & header = *main_plan.getCurrentHeader();
            ActionsDAG masks_dag(header.getColumnsWithTypeAndName());
            auto is_not_null = FunctionFactory::instance().get("isNotNull", context);

            std::map<size_t, String> mask_names;
            for (const auto & [stuffing, source] : mask_sources)
            {
                auto [it, inserted] = mask_names.emplace(source, makeMaskName(source));
                if (inserted)
                {
                    const auto * index = masks_dag.getInputs()[header.getPositionByName(index_names.at(source))];
                    masks_dag.getOutputs().push_back(&masks_dag.addFunction(is_not_null, {index}, it->second));
                }
                masks.emplace(stuffing, it->second);
            }

            auto step = std::make_unique<ExpressionStep>(main_plan.getCurrentHeader(), std::move(masks_dag));
            step->setStepDescription("Rows the joins matched");
            main_plan.addStep(std::move(step));
        }

        /// One lazy read per source, each looking its rows up by that source's row index.
        std::unordered_map<const ActionsDAG::Node *, String> lazy_column_names;
        for (auto & [source, lazy] : lazy_by_source)
        {
            if (!lazy.merge_tree_reading && !lazy.object_storage_reading)
                continue;

            QueryPlan lazy_plan;
            ILazyMaterializingRowsPtr lazy_materializing_rows;
            if (lazy.merge_tree_reading)
            {
                auto rows = std::make_shared<LazyMaterializingRows>(std::move(lazy.parts));
                lazy.merge_tree_reading->setLazyMaterializingRows(rows);
                lazy_materializing_rows = std::move(rows);
                lazy_plan.addStep(std::move(lazy.merge_tree_reading));
            }
            else
            {
                auto rows = std::make_shared<ObjectStorageLazyMaterializingRows>(lazy.row_index_registry);
                lazy.object_storage_reading->setLazyMaterializingRows(rows);
                lazy_materializing_rows = std::move(rows);
                lazy_plan.addStep(std::move(lazy.object_storage_reading));
            }

            {
                const auto & lazy_header = *lazy_plan.getCurrentHeader();
                ActionsDAG renaming(lazy_header.getColumnsWithTypeAndName());
                auto & renaming_outputs = renaming.getOutputs();
                for (auto & output : renaming_outputs)
                    output = &renaming.addAlias(*output, makeUniqueName(lazy_column_prefix, source, output->result_name));

                for (const auto * input : lazy.inputs)
                    if (lazy_header.has(input->result_name))
                        lazy_column_names.emplace(input, makeUniqueName(lazy_column_prefix, source, input->result_name));

                auto step = std::make_unique<ExpressionStep>(lazy_plan.getCurrentHeader(), std::move(renaming));
                step->setStepDescription("Names of the lazily read columns");
                lazy_plan.addStep(std::move(step));
            }

            auto join_lazy_columns = std::make_unique<JoinLazyColumnsStep>(
                main_plan.getCurrentHeader(), lazy_plan.getCurrentHeader(), lazy_materializing_rows, lazy.index_name);

            QueryPlan joined;
            std::vector<QueryPlanPtr> plans;
            plans.emplace_back(std::make_unique<QueryPlan>(std::move(main_plan)));
            plans.emplace_back(std::make_unique<QueryPlan>(std::move(lazy_plan)));
            joined.unitePlans(std::move(join_lazy_columns), std::move(plans));
            main_plan = std::move(joined);
        }

        /// Above the `LIMIT`, compute what the subtree produced from what crossed and what the lazy reads
        /// returned. Every other column is consumed, so the header comes out exactly as it was.
        {
            std::unordered_map<const ActionsDAG::Node *, String> inputs;
            for (const auto * sort_key : sort_key_nodes)
                inputs.emplace(sort_key, sort_key->result_name);
            for (const auto & crossing : crossings)
                inputs.emplace(crossing.node, crossing.name);
            for (const auto & [input, name] : lazy_column_names)
                inputs.emplace(input, name);

            auto above = buildAboveLimitDAG(merged, frontier, inputs, masks, *main_plan.getCurrentHeader(), context);

            auto step = std::make_unique<ExpressionStep>(main_plan.getCurrentHeader(), std::move(above));
            step->setStepDescription("Computed after the LIMIT");
            main_plan.addStep(std::move(step));
        }

        query_plan.replaceNodeWithPlan(&root, std::move(main_plan), std::move(expected_header));
    }

private:
    struct LazySource
    {
        /// The source columns the main read keeps, and the ones the lazy read is to fetch.
        NameSet eager_names;
        std::vector<const ActionsDAG::Node *> inputs;

        /// Set by `apply`. A source keeps none of these when its read turns out to keep every column.
        String index_name;
        std::unique_ptr<LazilyReadFromMergeTree> merge_tree_reading;
        std::unique_ptr<LazilyReadFromObjectStorage> object_storage_reading;
        RangesInDataParts parts;
        LazyObjectStorageFileRegistryPtr row_index_registry;
    };

    /// Whether every step between `node` and the sources is one this rebuilds.
    bool checkSteps(QueryPlan::Node * node, size_t & joins)
    {
        for (const auto & column : *node->step->getOutputHeader())
            if (isNameOfThisPass(column.name))
                return false;

        if (source_numbers.contains(node))
            return true;

        auto * step = node->step.get();
        if (auto * join = typeid_cast<JoinStepLogical *>(step))
        {
            if (node->children.size() != 2 || !join->canRemoveUnusedColumns())
                return false;

            /// The columns a join reads are selected from the rebuilt sides by name.
            for (const auto * child : node->children)
                if (!hasUniqueNames(*child->step->getOutputHeader()))
                    return false;

            ++joins;
            join_nodes.push_back(node);
            return checkSteps(node->children.front(), joins) && checkSteps(node->children.back(), joins);
        }

        if (node->children.size() != 1)
            return false;

        const ActionsDAG * dag = nullptr;
        if (auto * expression = typeid_cast<ExpressionStep *>(step))
        {
            /// Such a step keeps inputs nothing reads, which the main branch below it would not have.
            if (expression->isInputRemovalPrevented())
                return false;
            dag = &expression->getExpression();
        }
        else if (auto * filter = typeid_cast<FilterStep *>(step))
        {
            if (filter->isInputRemovalPrevented())
                return false;
            dag = &filter->getExpression();
        }
        else
            return false;

        /// A rebuilt step consumes by name what the original consumed, which a name read twice makes
        /// ambiguous.
        NameSet input_names;
        for (const auto * input : dag->getInputs())
            if (!input_names.insert(input->result_name).second)
                return false;

        return checkSteps(node->children.front(), joins);
    }

    /// The outputs of a join the main branch still needs.
    std::vector<size_t> findKeptJoinOutputs(const QueryPlan::Node & join_node) const
    {
        const auto & join = typeid_cast<const JoinStepLogical &>(*join_node.step);
        const auto & to_merged = merged.step_mappings.at(&join_node);
        const auto & join_outputs = join.getActionsDAG().getOutputs();

        std::vector<size_t> kept;
        for (size_t position = 0; position < join_outputs.size(); ++position)
            if (needed_below.contains(to_merged.at(join_outputs[position])))
                kept.push_back(position);
        return kept;
    }

    /// Whether the columns the pruned join reads are all ones the rebuilt sides produce, which are the ones
    /// needed below the `LIMIT` under their own names. A join keeps a column of a side it reads nothing of, so that
    /// the side does not lose every column, and that one need not be among them.
    bool canPruneJoin(const QueryPlan::Node & join_node) const
    {
        const auto & join = typeid_cast<const JoinStepLogical &>(*join_node.step);
        const auto & to_merged = merged.step_mappings.at(&join_node);
        const auto required = join.getRequiredColumns(findKeptJoinOutputs(join_node), /*remove_inputs=*/true);

        std::unordered_map<std::string_view, const ActionsDAG::Node *> inputs_by_name;
        for (const auto * input : join.getActionsDAG().getInputs())
            inputs_by_name.emplace(input->result_name, input);

        for (size_t side = 0; side < 2; ++side)
        {
            const auto & header = *join.getInputHeaders().at(side);

            std::vector<size_t> positions;
            if (required.required_input_positions.empty())
            {
                positions.resize(header.columns());
                std::iota(positions.begin(), positions.end(), 0);
            }
            else
                positions = required.required_input_positions.at(side);

            for (size_t position : positions)
            {
                const auto it = inputs_by_name.find(header.getByPosition(position).name);
                if (it == inputs_by_name.end())
                    return false;

                const auto mapped = to_merged.find(it->second);
                if (mapped == to_merged.end() || !needed_below.contains(mapped->second))
                    return false;
            }
        }

        return true;
    }

    /// Names this rebuild adds to the main branch, which the joins pass through.
    bool isAddedColumn(const String & name) const { return added_names.contains(name); }

    QueryPlan rebuild(QueryPlan::Node * node, QueryPlan::Nodes & nodes)
    {
        if (const auto it = source_numbers.find(node); it != source_numbers.end())
            return rebuildSource(node, it->second, nodes);

        if (typeid_cast<JoinStepLogical *>(node->step.get()))
            return rebuildJoin(node, nodes);

        const auto crossing_here = nameCrossingsOf(node);
        const auto & to_merged = merged.step_mappings.at(node);

        auto plan = rebuild(node->children.front(), nodes);
        const auto & header = *plan.getCurrentHeader();

        QueryPlanStepPtr step;
        if (auto * expression = typeid_cast<ExpressionStep *>(node->step.get()))
        {
            auto dag = rebuildStepDAG(expression->getExpression(), to_merged, needed_below, crossing_here, header);
            step = std::make_unique<ExpressionStep>(plan.getCurrentHeader(), std::move(dag));
        }
        else
        {
            auto * filter = typeid_cast<FilterStep *>(node->step.get());
            auto dag = rebuildStepDAG(filter->getExpression(), to_merged, needed_below, crossing_here, header);
            step = std::make_unique<FilterStep>(
                plan.getCurrentHeader(), std::move(dag), filter->getFilterColumnName(), filter->removesFilterColumn());
        }

        step->setStepDescription(*node->step);
        plan.addStep(std::move(step));
        return plan;
    }

    /// Names the crossings whose value `node` computes, and returns them as the nodes of its step's own DAG
    /// they are copies of, which is the form `rebuildStepDAG` takes them in. A source has no DAG of its own,
    /// so for a source these are the inputs of the merged DAG.
    std::vector<std::pair<const ActionsDAG::Node *, String>> nameCrossingsOf(const QueryPlan::Node * node)
    {
        std::vector<std::pair<const ActionsDAG::Node *, String>> exported;
        for (auto & crossing : crossings)
        {
            if (!crossing.name.empty())
                continue;

            const auto & origin = merged.getOrigin(crossing.node);
            if (origin.plan_node != node)
                continue;

            crossing.name = makeUniqueName(crossing_column_prefix, next_crossing_name++, crossing.node->result_name);
            added_names.insert(crossing.name);
            exported.emplace_back(origin.step_node ? origin.step_node : crossing.node, crossing.name);
        }
        return exported;
    }

    QueryPlan rebuildSource(QueryPlan::Node * node, size_t source, QueryPlan::Nodes & nodes)
    {
        /// Whatever is below the source stays as it is.
        auto plan = QueryPlan::extractSubplan(node, nodes);

        auto * merge_tree = typeid_cast<ReadFromMergeTree *>(plan.getRootNode()->step.get());
        auto * object_storage = typeid_cast<ReadFromObjectStorageStep *>(plan.getRootNode()->step.get());

        std::vector<const ActionsDAG::Node *> crossing_inputs;
        const auto lazy_it = lazy_by_source.find(source);
        if (lazy_it != lazy_by_source.end())
        {
            auto & lazy = lazy_it->second;
            if (merge_tree)
                lazy.merge_tree_reading = merge_tree->keepOnlyRequiredColumnsAndCreateLazyReadStep(lazy.eager_names);
            else
                lazy.object_storage_reading = object_storage->keepOnlyRequiredColumnsAndCreateLazyReadStep(lazy.eager_names);

            const bool reads_lazily = lazy.merge_tree_reading || lazy.object_storage_reading;
            const Block * lazy_header = nullptr;
            if (lazy.merge_tree_reading)
                lazy_header = lazy.merge_tree_reading->getOutputHeader().get();
            else if (lazy.object_storage_reading)
                lazy_header = lazy.object_storage_reading->getOutputHeader().get();

            /// A read keeps more than it is asked to - PREWHERE and row policy inputs, virtual columns,
            /// columns an object storage `DEFAULT` expression reads - and may keep everything. A column meant
            /// for the lazy read that the main read kept is read there already, so it crosses instead.
            for (const auto * input : lazy.inputs)
                if (!reads_lazily || !lazy_header->has(input->result_name))
                    crossing_inputs.push_back(input);

            if (reads_lazily)
            {
                lazy.index_name = makeRowIndexName(source);

                if (merge_tree)
                {
                    /// The lazy read fetches exactly the rows the main read selected, addressed by their row
                    /// index. It must not apply the vector search rescoring filter again: that one belongs to
                    /// the main read, and applying it against the candidates of the vector index can drop a
                    /// requested row.
                    lazy.parts = merge_tree->getParts();
                    for (auto & part : lazy.parts)
                        part.read_hints.use_vector_search_result_filter = false;
                }
                else
                    lazy.row_index_registry = object_storage->getLazyRowIndexRegistry();
            }
        }

        for (const auto * input : crossing_inputs)
            crossings.push_back({input, String()});

        /// A source that masks a stuffing needs its row index even where nothing of it is read lazily.
        const bool reads_lazily = lazy_it != lazy_by_source.end() && !lazy_it->second.index_name.empty();
        const bool masks_stuffing = std::ranges::any_of(mask_sources, [&](const auto & mask_source) { return mask_source.second == source; });
        const bool has_index = reads_lazily || masks_stuffing;
        if (has_index)
        {
            index_names.emplace(source, makeRowIndexName(source));
            added_names.insert(makeRowIndexName(source));
        }

        /// Each crossing value read from the source is exported right above it, together with the row index.
        const auto exported = nameCrossingsOf(node);
        if (!has_index && exported.empty())
            return plan;

        /// Below a join that can leave the source unmatched the index is `Nullable`; every column of a source
        /// is gated alike.
        const auto & source_inputs = merged.sources[source].inputs;
        const bool nullable_index = !source_inputs.empty() && merged.getNearestStuffing(source_inputs.front()) != nullptr;

        /// Asking a MergeTree read for the offsets the row index is computed from changes its header, so the
        /// header is taken only once that is done.
        ActionsDAG above_read = has_index
            ? makeRowIndexDAG(merge_tree, *plan.getCurrentHeader(), makeRowIndexName(source), nullable_index)
            : ActionsDAG();
        const auto read_header = plan.getCurrentHeader();

        for (const auto & [input_node, name] : exported)
        {
            /// The row index reads `_part_offset` and `_part_starting_offset` already, and a query can select
            /// those as well, in which case they cross like any other column read eagerly.
            const auto & column_name = input_node->result_name;
            const auto & inputs = above_read.getInputs();
            const auto existing = std::ranges::find_if(inputs, [&](const auto * input) { return input->result_name == column_name; });

            const ActionsDAG::Node * input = nullptr;
            if (existing != inputs.end())
            {
                input = *existing;
                if (std::ranges::find(above_read.getOutputs(), input) == above_read.getOutputs().end())
                    above_read.getOutputs().push_back(input);
            }
            else
            {
                input = &above_read.addInput(read_header->getByName(column_name));
                above_read.getOutputs().push_back(input);
            }

            above_read.getOutputs().push_back(&above_read.addAlias(*input, name));
        }

        auto step = std::make_unique<ExpressionStep>(plan.getCurrentHeader(), std::move(above_read));
        if (has_index)
            step->setStepDescription("Row index and columns crossing the LIMIT");
        else
            step->setStepDescription("Columns crossing the LIMIT");
        plan.addStep(std::move(step));
        return plan;
    }

    QueryPlan rebuildJoin(QueryPlan::Node * node, QueryPlan::Nodes & nodes)
    {
        const auto kept = findKeptJoinOutputs(*node);

        std::vector<QueryPlanPtr> sides;
        sides.emplace_back(std::make_unique<QueryPlan>(rebuild(node->children.front(), nodes)));
        sides.emplace_back(std::make_unique<QueryPlan>(rebuild(node->children.back(), nodes)));

        auto & join = typeid_cast<JoinStepLogical &>(*node->step);
        join.removeUnusedColumns(kept, /*remove_inputs=*/true);

        /// Each side hands the join exactly the columns it reads, and then the ones this rebuild added below,
        /// which the join passes through.
        ColumnsWithTypeAndName passed_through[2];
        for (size_t side = 0; side < 2; ++side)
        {
            auto & plan = *sides[side];
            const auto & header = *plan.getCurrentHeader();
            const auto & read = *join.getInputHeaders().at(side);

            Names names;
            for (const auto & column : read)
                names.push_back(column.name);
            for (const auto & column : header)
            {
                if (!isAddedColumn(column.name))
                    continue;
                names.push_back(column.name);
                passed_through[side].push_back(column);
            }

            if (header.getNames() == names)
                continue;

            ActionsDAG projection(header.getColumnsWithTypeAndName());
            const auto inputs = projection.getInputs();
            auto & projection_outputs = projection.getOutputs();
            projection_outputs.clear();
            for (const auto & name : names)
                projection_outputs.push_back(inputs[header.getPositionByName(name)]);

            auto step = std::make_unique<ExpressionStep>(plan.getCurrentHeader(), std::move(projection));
            step->setStepDescription("Columns the join reads");
            plan.addStep(std::move(step));
        }

        for (size_t side = 0; side < 2; ++side)
            for (const auto & column : passed_through[side])
                join.addPassThroughColumn(column, side == 0 ? JoinTableSide::Left : JoinTableSide::Right);

        join.updateInputHeaders({sides[0]->getCurrentHeader(), sides[1]->getCurrentHeader()});

        QueryPlan plan;
        plan.unitePlans(std::move(node->step), std::move(sides));
        return plan;
    }

    const MergedPlanDAG & merged;
    const LazyFrontier & frontier;
    const std::vector<bool> & lazy_sources;
    const std::unordered_map<const MergedPlanDAG::Stuffing *, size_t> & mask_sources;
    ContextPtr context;

    /// The row index of every source that has one, either for a lazy read or for a mask.
    std::map<size_t, String> index_names;

    std::unordered_map<const QueryPlan::Node *, size_t> source_numbers;
    std::vector<const QueryPlan::Node *> join_nodes;
    NodeSet sort_key_nodes;
    NodeSet needed_below;
    std::map<size_t, LazySource> lazy_by_source;

    /// Named as the rebuild reaches the step or the source that exports each.
    std::vector<Crossing> crossings;
    size_t next_crossing_name = 0;
    NameSet added_names;
};

}

bool optimizeLazyMaterialization3(
    QueryPlan::Node & root, QueryPlan & query_plan, QueryPlan::Nodes & nodes,
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

    /// Without ORDER BY, the `LIMIT` stops reading early, and whether a second read saves anything depends
    /// on how many rows the filters and joins drop on the way. Not handled yet. This runs before reading in
    /// order is applied, so the sorting is a full one.
    auto * sorting_node = root.children.front();
    auto * sorting_step = typeid_cast<SortingStep *>(sorting_node->step.get());
    if (!sorting_step || sorting_step->getType() != SortingStep::Type::Full)
        return false;

    auto * chain_top = sorting_node->children.front();
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
    for (const auto & description : sorting_step->getSortDescription())
    {
        const auto it = std::ranges::find_if(outputs, [&](const auto * output) { return output->result_name == description.column_name; });
        if (it == outputs.end())
            return false;
        sort_key_positions.push_back(it - outputs.begin());
    }
    std::ranges::sort(sort_key_positions);
    sort_key_positions.erase(std::unique(sort_key_positions.begin(), sort_key_positions.end()), sort_key_positions.end());

    /// A stuffing can be masked by the row index of a MergeTree source it stuffs directly, with no other
    /// stuffing in between: that index is NULL exactly at the rows it, or a join above it, left unmatched.
    std::unordered_map<const MergedPlanDAG::Stuffing *, size_t> mask_candidates;
    ContextPtr context;
    for (size_t source = 0; source < merged.sources.size(); ++source)
    {
        const auto & inputs = merged.sources[source].inputs;
        auto * merge_tree = typeid_cast<ReadFromMergeTree *>(merged.sources[source].plan_node->step.get());
        if (!lazy_sources[source] || !merge_tree || inputs.empty())
            continue;

        if (const auto * stuffing = merged.getNearestStuffing(inputs.front()))
        {
            mask_candidates.emplace(stuffing, source);
            context = merge_tree->getContext();
        }
    }

    std::unordered_set<const MergedPlanDAG::Stuffing *> masked_stuffings;
    for (const auto & [stuffing, source] : mask_candidates)
        masked_stuffings.insert(stuffing);

    const auto frontier = chooseLazyFrontier(merged, sort_key_positions, lazy_sources, masked_stuffings);
    if (!frontier.defersAnything())
        return false;

    /// Only the masks the frontier uses are computed.
    std::unordered_map<const MergedPlanDAG::Stuffing *, size_t> mask_sources;
    for (const auto & node : merged.getDAG().getNodes())
        if (isMaskedRecomputation(merged, frontier, &node))
            mask_sources.emplace(merged.getNearestStuffing(&node), mask_candidates.at(merged.getNearestStuffing(&node)));

    JoinPlanRebuild rebuild(merged, frontier, lazy_sources, mask_sources, context);
    if (!rebuild.prepare(chain_top, sort_key_positions))
        return false;

    rebuild.apply(query_plan, nodes, root, *sorting_node, chain_top, sort_key_positions);
    return true;
}

}
