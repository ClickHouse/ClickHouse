#include <Processors/QueryPlan/Optimizations/Optimizations.h>

#include <DataTypes/DataTypesNumber.h>
#include <Processors/QueryPlan/BuildRuntimeFilterStep.h>
#include <Functions/FunctionFactory.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinLazyColumnsStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/LazilyReadFromMergeTree.h>
#include <Processors/QueryPlan/LazilyReadFromObjectStorage.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/Optimizations/actionsDAGUtils.h>
#include <Processors/QueryPlan/Optimizations/lazyFrontier.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/ReadFromObjectStorageStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/Transforms/LazyMaterializingTransform.h>
#include <Common/typeid_cast.h>

#include <map>
#include <numeric>
#include <set>

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

/// Whether a join matched a row, computed from the row index of a source the join can leave unmatched.
constexpr std::string_view mask_column_prefix = "__lazy_mask_";

String makeRowIndexName(size_t source)
{
    return fmt::format("{}_{}", global_row_index_name, source);
}

String makeMaskName(size_t source)
{
    return fmt::format("{}{}", mask_column_prefix, source);
}

bool isNameOfThisPass(std::string_view name)
{
    return name.starts_with(global_row_index_name) || name.starts_with(mask_column_prefix);
}

bool hasUniqueNames(const Block & header)
{
    NameSet names;
    for (const auto & column : header)
        if (!names.insert(column.name).second)
            return false;
    return true;
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

/// The expression computed above the `LIMIT`: the outputs of `merged`, computed from the columns of
/// `header`. `positions` says which column holds a value that crossed or was read lazily, and `masks` which
/// one holds the mask of a stuffing; every other value is recomputed, masked where `isMaskedRecomputation`
/// says so. The columns are bound by position, so names that repeat - two lazy reads of columns with the
/// same name - do not matter. Every column is consumed, so the result has exactly the header the subtree
/// had.
ActionsDAG buildAboveLimitDAG(
    const MergedPlanDAG & merged,
    const LazyFrontier & frontier,
    const std::unordered_map<const ActionsDAG::Node *, size_t> & positions,
    const std::unordered_map<const MergedPlanDAG::Stuffing *, size_t> & masks,
    const Block & header,
    const ContextPtr & context)
{
    ActionsDAG dag(header.getColumnsWithTypeAndName());
    const auto inputs = dag.getInputs();
    std::unordered_map<const ActionsDAG::Node *, const ActionsDAG::Node *> copies;

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

            if (const auto it = positions.find(node); it != positions.end())
            {
                copies.emplace(node, inputs.at(it->second));
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
                const auto * mask = inputs.at(masks.at(merged.getNearestStuffing(node)));
                copy = &addMasked(dag, *mask, *copy, node->result_name, context);
            }

            copies.emplace(node, copy);
            stack.pop_back();
        }
    }

    auto & dag_outputs = dag.getOutputs();
    dag_outputs.clear();
    for (const auto * output : merged.getOutputs())
    {
        const auto * copy = copies.at(output);
        if (copy->result_name != output->result_name)
            copy = &dag.addAlias(*copy, output->result_name);
        dag_outputs.push_back(copy);
    }

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

/// Rebuilds `Limit -> Sorting -> tree of Expression, Filter, BuildRuntimeFilter and JoinStepLogical ->
/// sources` so that the columns the frontier defers are read for the rows the `LIMIT` returns only.
///
/// The main branch is the original plan with fewer columns: every step is kept, with its own expressions
/// and names, and `removeUnusedColumns` takes away what nothing below the `LIMIT` needs any more, top-down.
/// A value that crosses the `LIMIT` keeps the name the plan gives it, and is kept by each step it passes.
/// Where a step used to consume it, because what the step computed from it is now computed above the
/// `LIMIT`, the step outputs it as well. Each lazily read source adds its row index above the read, which
/// the joins pass through, and above the `LIMIT` one `JoinLazyColumnsStep` per such source reads its
/// deferred columns by that index.
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

    bool prepare(QueryPlan::Node * chain_top_, const std::vector<size_t> & sort_key_positions)
    {
        chain_top = chain_top_;

        size_t joins = 0;
        if (!checkSteps(chain_top, joins))
            return false;

        /// A subtree reading one source is what `optimizeLazyMaterialization2` handles.
        if (joins == 0)
            return false;

        const auto & outputs = merged.getOutputs();
        for (size_t position : sort_key_positions)
            sort_key_nodes.insert(outputs[position]);

        std::vector<const ActionsDAG::Node *> kept_by_read;
        if (!chooseLazyReads(kept_by_read))
            return false;

        for (const auto & node : merged.getDAG().getNodes())
            if (isMaskedRecomputation(merged, frontier, &node) && !canMask(node, context))
                return false;

        /// The values that cross the `LIMIT`: what the frontier says, and the columns meant for a lazy read
        /// that the main read keeps anyway. A sort key crosses as a column the sorting needs.
        std::vector<const ActionsDAG::Node *> crossing_values = std::move(kept_by_read);
        for (const auto & [node, placed] : frontier.placement)
            if (placed.above == Placement::Above::Crossing && !sort_key_nodes.contains(node))
                crossing_values.push_back(node);

        /// A value carries the name of the step it is computed in, and a step above may compute another value
        /// under the same name - `join_use_nulls` makes a join output `toNullable(x)` as `x`. So where the only
        /// thing above the `LIMIT` that reads a crossing value is a node computed from it alone, that node
        /// crosses instead. The plan computes it anyway, in its own step, and the column it is carried in is
        /// the one the rest of the plan knows: the analyzer's rename of a table's column right above the read,
        /// the join's `toNullable`, a subquery's output.
        std::unordered_map<const ActionsDAG::Node *, std::vector<const ActionsDAG::Node *>> parents_of;
        for (const auto & node : merged.getDAG().getNodes())
            for (const auto * child : node.children)
                parents_of[child].push_back(&node);

        const NodeSet outputs_set(outputs.begin(), outputs.end());
        const auto lift = [&](const ActionsDAG::Node * value) -> const ActionsDAG::Node *
        {
            if (outputs_set.contains(value))
                return nullptr;

            const auto it = parents_of.find(value);
            if (it == parents_of.end())
                return nullptr;

            const ActionsDAG::Node * reader = nullptr;
            for (const auto * parent : it->second)
            {
                if (frontier.at(parent).above != Placement::Above::Recomputed || parent == reader)
                    continue;
                if (reader)
                    return nullptr;
                reader = parent;
            }

            if (!reader)
                return nullptr;

            if (reader->type == ActionsDAG::ActionType::FUNCTION)
            {
                if (!reader->function_base->isDeterministicInScopeOfQuery() || reader->function_base->isStateful())
                    return nullptr;
                for (const auto * child : reader->children)
                    if (child != value && child->type != ActionsDAG::ActionType::COLUMN)
                        return nullptr;
            }
            else if (reader->type != ActionsDAG::ActionType::ALIAS)
                return nullptr;

            return reader;
        };

        for (const auto * value : crossing_values)
        {
            /// The nodes that are the same value as the one carried: an alias is, a function of it is not.
            std::vector<const ActionsDAG::Node *> same_value{value};
            const auto * carried = value;
            while (const auto * reader = lift(carried))
            {
                if (reader->type != ActionsDAG::ActionType::ALIAS)
                    same_value.clear();
                same_value.push_back(reader);
                carried = reader;
            }

            for (const auto * link : same_value)
                carried_as.emplace(link, carried);

            if (!sort_key_nodes.contains(carried) && std::ranges::find(carried_values, carried) == carried_values.end())
                carried_values.push_back(carried);
        }

        /// What something below the `LIMIT` reads.
        {
            ActionsDAG::NodeRawConstPtrs roots = merged.filter_nodes;
            roots.append_range(merged.join_condition_nodes);
            roots.append_range(merged.step_read_nodes);
            roots.append_range(sort_key_nodes);
            roots.append_range(carried_values);
            needed = findReachableNodes(roots);
        }

        NameSet carried_names;
        for (const auto * value : carried_values)
            if (!carried_names.insert(value->result_name).second || !planCarrying(value))
                return false;

        return true;
    }

    void apply(QueryPlan & query_plan, QueryPlan::Nodes & nodes, QueryPlan::Node & root, QueryPlan::Node & sorting_node,
        const std::vector<size_t> & sort_key_positions)
    {
        /// The values carried up are added to the outputs of the steps that would drop them, and the headers
        /// above are brought in line.
        addCarriedOutputs(chain_top);

        /// What the chain hands over: the sort keys and the values crossing the `LIMIT`.
        std::vector<size_t> required;
        {
            const auto & values = header_values.at(chain_top);
            for (size_t position = 0; position < values.size(); ++position)
                if (sort_key_nodes.contains(values[position]) || std::ranges::find(carried_values, values[position]) != carried_values.end())
                    required.push_back(position);
        }
        prune(chain_top, required);

        auto main_plan = assemble(chain_top, nodes);

        /// Hand over exactly the sort keys, the crossing values and the row indexes; the sort then carries no
        /// more than that. What each column of the block holds is followed from here on, so that the
        /// expression above the `LIMIT` can bind by position.
        const auto & outputs = merged.getOutputs();
        std::vector<const ActionsDAG::Node *> slots;
        std::vector<String> slot_indexes;
        {
            const auto & header = *main_plan.getCurrentHeader();
            ActionsDAG projection(header.getColumnsWithTypeAndName());
            const auto inputs = projection.getInputs();
            auto find_input = [&](const String & name) { return inputs[header.getPositionByName(name)]; };

            auto & projection_outputs = projection.getOutputs();
            projection_outputs.clear();
            for (size_t position : sort_key_positions)
            {
                projection_outputs.push_back(find_input(outputs[position]->result_name));
                slots.push_back(outputs[position]);
                slot_indexes.emplace_back();
            }
            for (const auto * value : carried_values)
            {
                projection_outputs.push_back(find_input(value->result_name));
                slots.push_back(value);
                slot_indexes.emplace_back();
            }
            for (const auto & [source, index_name] : index_names)
            {
                projection_outputs.push_back(find_input(index_name));
                slots.push_back(nullptr);
                slot_indexes.push_back(index_name);
            }

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
        if (!mask_sources.empty())
        {
            const auto & header = *main_plan.getCurrentHeader();
            ActionsDAG masks_dag(header.getColumnsWithTypeAndName());
            auto is_not_null = FunctionFactory::instance().get("isNotNull", context);

            /// A mask column serves every stuffing its source masks.
            std::map<size_t, size_t> mask_slot_of_source;
            for (const auto & [stuffing, source] : mask_sources)
            {
                auto [it, inserted] = mask_slot_of_source.emplace(source, slots.size());
                if (inserted)
                {
                    const auto * index = masks_dag.getInputs()[header.getPositionByName(index_names.at(source))];
                    masks_dag.getOutputs().push_back(&masks_dag.addFunction(is_not_null, {index}, makeMaskName(source)));
                    slots.push_back(nullptr);
                    slot_indexes.emplace_back();
                }
                mask_slots.emplace(stuffing, it->second);
            }

            auto step = std::make_unique<ExpressionStep>(main_plan.getCurrentHeader(), std::move(masks_dag));
            step->setStepDescription("Rows the joins matched");
            main_plan.addStep(std::move(step));
        }

        /// One lazy read per source, each looking its rows up by that source's row index. The step takes the
        /// index away and appends what it read on the right.
        for (auto & [source, lazy] : lazy_by_source)
        {
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

            const auto & index_name = index_names.at(source);
            const auto index_slot = std::ranges::find(slot_indexes, index_name) - slot_indexes.begin();
            slots.erase(slots.begin() + index_slot);
            slot_indexes.erase(slot_indexes.begin() + index_slot);
            for (auto & [stuffing, slot] : mask_slots)
                if (slot > static_cast<size_t>(index_slot))
                    --slot;

            for (const auto & column : *lazy_plan.getCurrentHeader())
            {
                const auto & inputs = merged.sources[source].inputs;
                const auto it = std::ranges::find_if(inputs, [&](const auto * input) { return input->result_name == column.name; });
                slots.push_back(it == inputs.end() ? nullptr : *it);
                slot_indexes.emplace_back();
            }

            auto join_lazy_columns = std::make_unique<JoinLazyColumnsStep>(
                main_plan.getCurrentHeader(), lazy_plan.getCurrentHeader(), lazy_materializing_rows, index_name);

            QueryPlan joined;
            std::vector<QueryPlanPtr> plans;
            plans.emplace_back(std::make_unique<QueryPlan>(std::move(main_plan)));
            plans.emplace_back(std::make_unique<QueryPlan>(std::move(lazy_plan)));
            joined.unitePlans(std::move(join_lazy_columns), std::move(plans));
            main_plan = std::move(joined);
        }

        /// Above the `LIMIT`, compute what the subtree produced from what crossed and what the lazy reads
        /// returned.
        {
            const auto & header = *main_plan.getCurrentHeader();
            if (header.columns() != slots.size())
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Lazy materialization expected {} columns above the LIMIT, got {}", slots.size(), header.dumpNames());

            std::unordered_map<const ActionsDAG::Node *, size_t> positions;
            for (size_t position = 0; position < slots.size(); ++position)
                if (slots[position])
                    positions.emplace(slots[position], position);

            /// A value crosses as the column it is carried in, which may be a rename of it.
            for (const auto & [value, carried] : carried_as)
                if (const auto it = positions.find(carried); it != positions.end())
                    positions.emplace(value, it->second);

            auto above = buildAboveLimitDAG(merged, frontier, positions, mask_slots, header, context);

            auto step = std::make_unique<ExpressionStep>(main_plan.getCurrentHeader(), std::move(above));
            step->setStepDescription("Computed after the LIMIT");
            main_plan.addStep(std::move(step));
        }

        query_plan.replaceNodeWithPlan(&root, std::move(main_plan), std::move(expected_header));
    }

private:
    struct LazySource
    {
        /// The source columns the main read keeps, and the ones the lazy read fetches.
        NameSet eager_names;
        std::vector<const ActionsDAG::Node *> inputs;

        /// Set by `apply`.
        std::unique_ptr<LazilyReadFromMergeTree> merge_tree_reading;
        std::unique_ptr<LazilyReadFromObjectStorage> object_storage_reading;
        RangesInDataParts parts;
        LazyObjectStorageFileRegistryPtr row_index_registry;
    };

    /// Whether every step between `node` and the sources is one this rebuilds.
    bool checkSteps(QueryPlan::Node * node, size_t & joins)
    {
        /// The steps are pruned and reconciled by name, which a name occurring twice makes ambiguous.
        const auto & header = *node->step->getOutputHeader();
        if (!hasUniqueNames(header))
            return false;
        for (const auto & column : header)
            if (isNameOfThisPass(column.name))
                return false;

        if (source_numbers.contains(node))
            return true;

        auto * step = node->step.get();
        for (auto * child : node->children)
            parents.emplace(child, node);

        /// Passes its input through, and reads its key by name, which the merged DAG has made sure is there once.
        if (typeid_cast<BuildRuntimeFilterStep *>(step))
            return node->children.size() == 1 && checkSteps(node->children.front(), joins);

        if (auto * join = typeid_cast<JoinStepLogical *>(step))
        {
            if (node->children.size() != 2 || !join->canRemoveUnusedColumns())
                return false;

            /// A join keeps a column of a side it reads nothing of, whichever comes first, and that could be one
            /// that is read lazily now. A condition reading each side rules that out.
            std::array<bool, 2> reads_side{false, false};
            const auto & join_operator = join->getJoinOperator();
            for (const auto * conditions : {&join_operator.expression, &join_operator.residual_filter})
            {
                for (const auto & condition : *conditions)
                {
                    const auto & condition_sources = condition.getSourceRelations();
                    reads_side[0] |= condition_sources.test(0);
                    reads_side[1] |= condition_sources.test(1);
                }
            }
            if (!reads_side[0] || !reads_side[1])
                return false;

            ++joins;
            return checkSteps(node->children.front(), joins) && checkSteps(node->children.back(), joins);
        }

        if (node->children.size() != 1)
            return false;

        if (auto * expression = typeid_cast<ExpressionStep *>(step))
        {
            /// Such a step keeps every input, including the ones a child no longer produces.
            if (expression->isInputRemovalPrevented() || !expression->canRemoveUnusedColumns())
                return false;
        }
        else if (auto * filter = typeid_cast<FilterStep *>(step))
        {
            if (filter->isInputRemovalPrevented() || !filter->canRemoveUnusedColumns())
                return false;
        }
        else
            return false;

        return checkSteps(node->children.front(), joins);
    }

    /// Picks the columns each source reads lazily. A column the frontier wants read lazily that something
    /// below the `LIMIT` reads as well, or that the read keeps anyway, stays in the main read and crosses the
    /// `LIMIT` as a column instead; those are returned in `kept_by_read`.
    bool chooseLazyReads(std::vector<const ActionsDAG::Node *> & kept_by_read)
    {
        for (size_t source = 0; source < merged.sources.size(); ++source)
        {
            if (!lazy_sources[source])
                continue;

            LazySource lazy;
            std::vector<const ActionsDAG::Node *> candidates;
            for (const auto * input : merged.sources[source].inputs)
            {
                const auto placed = frontier.at(input);
                if (placed.above == Placement::Above::LazyRead && !placed.computed_below)
                    candidates.push_back(input);
                else
                {
                    lazy.eager_names.insert(input->result_name);
                    if (placed.above == Placement::Above::LazyRead)
                        kept_by_read.push_back(input);
                }
            }

            if (candidates.empty())
                continue;

            const auto * step = merged.sources[source].plan_node->step.get();
            NameSet lazily_read;
            if (const auto * merge_tree = typeid_cast<const ReadFromMergeTree *>(step))
                lazily_read.insert_range(merge_tree->getLazilyReadColumns(lazy.eager_names));
            else
                lazily_read = typeid_cast<const ReadFromObjectStorageStep &>(*step).getLazilyReadColumns(lazy.eager_names);

            for (const auto * input : candidates)
            {
                if (lazily_read.contains(input->result_name))
                    lazy.inputs.push_back(input);
                else
                {
                    lazy.eager_names.insert(input->result_name);
                    kept_by_read.push_back(input);
                }
            }

            /// The lazy read fetches every column the main read leaves out, so the two have to agree.
            if (lazy.inputs.size() != lazily_read.size())
                return false;

            if (!lazy.inputs.empty())
                lazy_by_source.emplace(source, std::move(lazy));
        }

        return !lazy_by_source.empty();
    }

    /// Plans how `value` gets from the step computing it to the top of the chain under its own name: which
    /// steps have to output it besides what they did, and whether a column of the same name is in the way.
    bool planCarrying(const ActionsDAG::Node * value)
    {
        const auto & name = value->result_name;
        if (isNameOfThisPass(name))
            return false;

        const auto & origin = merged.getOrigin(value);
        const QueryPlan::Node * below = nullptr;
        for (const auto * node = origin.plan_node; node; below = node, node = parentOf(node))
        {
            const auto & values = merged.step_outputs.at(node);
            const auto & header = *node->step->getOutputHeader();

            /// Another column of the same name would be taken for this one.
            for (size_t position = 0; position < values.size(); ++position)
                if (header.getByPosition(position).name == name && values[position] != value)
                    return false;

            const bool is_output = std::ranges::find(values, value) != values.end();

            /// A column the step below outputs only now is new to this step, which passes it through by
            /// itself - a join only if it is told to.
            const bool arrives_new = below && std::ranges::find(merged.step_outputs.at(below), value) == merged.step_outputs.at(below).end();
            if (!is_output && arrives_new)
            {
                if (typeid_cast<const JoinStepLogical *>(node->step.get()))
                    join_pass_through[node].push_back(value);
            }
            else if (!is_output)
            {
                /// The step computes it, or reads it from below, and does not output it.
                const ActionsDAG::Node * step_node = nullptr;
                if (node == origin.plan_node)
                {
                    if (typeid_cast<const JoinStepLogical *>(node->step.get()))
                        return false;
                    step_node = origin.step_node;
                }
                else
                {
                    const auto mapping = merged.step_mappings.find(node);
                    if (mapping == merged.step_mappings.end())
                        return false;
                    for (const auto & [original, merged_node] : mapping->second)
                        if (original->type == ActionsDAG::ActionType::INPUT && merged_node == value)
                            step_node = original;
                }

                if (!step_node)
                    return false;
                added_outputs[node].emplace_back(step_node, value);
            }

            /// Left and right columns of a join never share a name.
            if (const auto * join = typeid_cast<const JoinStepLogical *>(node->step.get()); join && below)
            {
                const size_t other_side = node->children.front() == below ? 1 : 0;
                if (join->getInputHeaders().at(other_side)->has(name))
                    return false;
            }

            if (node == chain_top)
                return true;
        }

        return false;
    }

    const QueryPlan::Node * parentOf(const QueryPlan::Node * node) const
    {
        const auto it = parents.find(node);
        return it == parents.end() ? nullptr : it->second;
    }

    /// The value of each column of `node`'s output header, as it is now.
    ActionsDAG::NodeRawConstPtrs computeHeaderValues(const QueryPlan::Node * node) const
    {
        if (source_numbers.contains(node))
            return merged.step_outputs.at(node);

        const auto * step = node->step.get();
        if (typeid_cast<const BuildRuntimeFilterStep *>(step))
            return header_values.at(node->children.front());

        const auto & to_merged = merged.step_mappings.at(node);
        ActionsDAG::NodeRawConstPtrs values;

        if (const auto * join = typeid_cast<const JoinStepLogical *>(step))
        {
            for (const auto * output : join->getActionsDAG().getOutputs())
            {
                const auto passed = join_passed_values.find(output);
                values.push_back(passed != join_passed_values.end() ? passed->second : to_merged.at(output));
            }
            return values;
        }

        const ActionsDAG * dag = nullptr;
        String removed_filter_column;
        if (const auto * expression = typeid_cast<const ExpressionStep *>(step))
            dag = &expression->getExpression();
        else
        {
            const auto & filter = typeid_cast<const FilterStep &>(*step);
            dag = &filter.getExpression();
            if (filter.removesFilterColumn())
                removed_filter_column = filter.getFilterColumnName();
        }

        for (const auto * output : dag->getOutputs())
        {
            if (!removed_filter_column.empty() && output->result_name == removed_filter_column)
            {
                removed_filter_column.clear();
                continue;
            }
            values.push_back(to_merged.at(output));
        }

        /// Then what the step passes through without reading.
        const auto & child_values = header_values.at(node->children.front());
        const auto header_columns = mapHeaderColumnsToInputs(dag->getInputs(), *step->getInputHeaders().front());
        for (size_t position = 0; position < header_columns.size(); ++position)
            if (header_columns.passesThrough(position))
                values.push_back(child_values.at(position));

        return values;
    }

    /// Adds the outputs `planCarrying` found missing, bottom-up, and refreshes each header after its
    /// children's. Records what each column of each header holds.
    void addCarriedOutputs(QueryPlan::Node * node)
    {
        if (!source_numbers.contains(node))
        {
            for (auto * child : node->children)
                addCarriedOutputs(child);

            auto * step = node->step.get();
            if (const auto it = added_outputs.find(node); it != added_outputs.end())
            {
                for (const auto & [step_node, value] : it->second)
                {
                    if (auto * join = typeid_cast<JoinStepLogical *>(step))
                        join->addInputToOutputs(step_node);
                    else if (auto * expression = typeid_cast<ExpressionStep *>(step))
                        expression->getExpression().getOutputs().push_back(step_node);
                    else
                        typeid_cast<FilterStep &>(*step).getExpression().getOutputs().push_back(step_node);
                }
            }

            SharedHeaders input_headers;
            for (const auto * child : node->children)
                input_headers.push_back(child->step->getOutputHeader());

            /// A join passes a carried value through from the side it comes from.
            if (const auto it = join_pass_through.find(node); it != join_pass_through.end())
            {
                auto & join = typeid_cast<JoinStepLogical &>(*step);
                for (const auto * value : it->second)
                {
                    for (size_t side = 0; side < 2; ++side)
                    {
                        const auto & side_values = header_values.at(node->children[side]);
                        const auto position = std::ranges::find(side_values, value) - side_values.begin();
                        if (position == static_cast<ptrdiff_t>(side_values.size()))
                            continue;

                        join.addPassThroughColumn(
                            input_headers[side]->getByPosition(position), side == 0 ? JoinTableSide::Left : JoinTableSide::Right);
                        join_passed_values.emplace(join.getActionsDAG().getOutputs().back(), value);
                    }
                }
            }

            step->updateInputHeaders(std::move(input_headers));
        }

        auto values = computeHeaderValues(node);
        if (values.size() != node->step->getOutputHeader()->columns())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Lazy materialization lost track of the columns of {}: {} values for {}",
                node->step->getName(), values.size(), node->step->getOutputHeader()->dumpNames());
        header_values[node] = std::move(values);
    }

    /// Takes away, top-down, whatever the step above does not need of `node`'s output. The steps change only
    /// here; the reads, which choose what they keep themselves, change in `assemble`.
    void prune(QueryPlan::Node * node, const std::vector<size_t> & required)
    {
        if (const auto it = source_numbers.find(node); it != source_numbers.end())
            return;

        auto * step = node->step.get();
        std::vector<std::vector<size_t>> child_required;

        if (auto * runtime_filter = typeid_cast<BuildRuntimeFilterStep *>(step))
        {
            /// Passes its input through, so it needs what is needed above it, and its key.
            const auto & header = *node->children.front()->step->getOutputHeader();
            std::set<size_t> positions(required.begin(), required.end());
            positions.insert(header.getPositionByName(runtime_filter->getFilterColumnName()));
            child_required.emplace_back(positions.begin(), positions.end());
        }
        else
        {
            auto result = step->removeUnusedColumns(required, /*remove_inputs=*/true);
            child_required = std::move(result.required_input_positions);
        }

        for (size_t child = 0; child < node->children.size(); ++child)
        {
            std::vector<size_t> positions;
            if (child_required.empty())
            {
                positions.resize(node->children[child]->step->getOutputHeader()->columns());
                std::iota(positions.begin(), positions.end(), 0);
            }
            else
                positions = std::move(child_required[child]);

            prune(node->children[child], positions);
        }
    }

    bool isAddedColumn(const String & name) const { return added_names.contains(name); }

    /// Builds the main branch bottom-up from the pruned steps: splits the lazily read columns off the reads,
    /// adds the row indexes, which the joins pass through, and brings each step's input in line with what its
    /// child produces now.
    QueryPlan assemble(QueryPlan::Node * node, QueryPlan::Nodes & nodes)
    {
        if (const auto it = source_numbers.find(node); it != source_numbers.end())
            return assembleSource(node, it->second, nodes);

        std::vector<QueryPlanPtr> children;
        for (auto * child : node->children)
            children.emplace_back(std::make_unique<QueryPlan>(assemble(child, nodes)));

        auto * step = node->step.get();
        if (auto * join = typeid_cast<JoinStepLogical *>(step))
        {
            /// Each side hands the join exactly the columns it reads, and then the ones this rebuild added below,
            /// which the join passes through.
            ColumnsWithTypeAndName passed_through[2];
            for (size_t side = 0; side < 2; ++side)
            {
                auto & plan = *children[side];
                const auto & header = *plan.getCurrentHeader();

                Names names = join->getInputHeaders().at(side)->getNames();
                for (const auto & column : header)
                {
                    if (!isAddedColumn(column.name))
                        continue;
                    names.push_back(column.name);
                    passed_through[side].push_back(column);
                }

                if (header.getNames() != names)
                {
                    ActionsDAG projection(header.getColumnsWithTypeAndName());
                    const auto inputs = projection.getInputs();
                    auto & projection_outputs = projection.getOutputs();
                    projection_outputs.clear();
                    for (const auto & name : names)
                        projection_outputs.push_back(inputs[header.getPositionByName(name)]);

                    auto projection_step = std::make_unique<ExpressionStep>(plan.getCurrentHeader(), std::move(projection));
                    projection_step->setStepDescription("Discarding unused columns");
                    plan.addStep(std::move(projection_step));
                }
            }

            for (size_t side = 0; side < 2; ++side)
                for (const auto & column : passed_through[side])
                    join->addPassThroughColumn(column, side == 0 ? JoinTableSide::Left : JoinTableSide::Right);

            join->updateInputHeaders({children[0]->getCurrentHeader(), children[1]->getCurrentHeader()});
        }
        else if (typeid_cast<BuildRuntimeFilterStep *>(step))
        {
            step->updateInputHeader(children.front()->getCurrentHeader());
        }
        else
        {
            /// A child that could not leave out everything the step no longer reads - a read keeps what its
            /// PREWHERE needs - has its extra columns consumed here. The row indexes pass through.
            const auto & header = *children.front()->getCurrentHeader();
            const auto & expected = *step->getInputHeaders().front();
            auto & dag = typeid_cast<ExpressionStep *>(step) ? typeid_cast<ExpressionStep &>(*step).getExpression()
                                                             : typeid_cast<FilterStep &>(*step).getExpression();
            for (const auto & column : header)
                if (!expected.has(column.name) && !isAddedColumn(column.name))
                    dag.addInput(column.name, column.type);

            step->updateInputHeader(children.front()->getCurrentHeader());
        }

        QueryPlan plan;
        if (children.size() == 1)
        {
            plan = std::move(*children.front());
            plan.addStep(std::move(node->step));
        }
        else
            plan.unitePlans(std::move(node->step), std::move(children));
        return plan;
    }

    QueryPlan assembleSource(QueryPlan::Node * node, size_t source, QueryPlan::Nodes & nodes)
    {
        /// Whatever is below the source stays as it is.
        auto plan = QueryPlan::extractSubplan(node, nodes);

        auto * merge_tree = typeid_cast<ReadFromMergeTree *>(plan.getRootNode()->step.get());
        auto * object_storage = typeid_cast<ReadFromObjectStorageStep *>(plan.getRootNode()->step.get());

        const auto lazy_it = lazy_by_source.find(source);
        if (lazy_it != lazy_by_source.end())
        {
            auto & lazy = lazy_it->second;
            const Block * lazy_header = nullptr;
            if (merge_tree)
            {
                lazy.merge_tree_reading = merge_tree->keepOnlyRequiredColumnsAndCreateLazyReadStep(lazy.eager_names);
                if (lazy.merge_tree_reading)
                    lazy_header = lazy.merge_tree_reading->getOutputHeader().get();

                /// The lazy read fetches exactly the rows the main read selected, addressed by their row
                /// index. It must not apply the vector search rescoring filter again: that one belongs to
                /// the main read, and applying it against the candidates of the vector index can drop a
                /// requested row.
                lazy.parts = merge_tree->getParts();
                for (auto & part : lazy.parts)
                    part.read_hints.use_vector_search_result_filter = false;
            }
            else
            {
                lazy.object_storage_reading = object_storage->keepOnlyRequiredColumnsAndCreateLazyReadStep(lazy.eager_names);
                if (lazy.object_storage_reading)
                    lazy_header = lazy.object_storage_reading->getOutputHeader().get();
                lazy.row_index_registry = object_storage->getLazyRowIndexRegistry();
            }

            if (!lazy_header || lazy_header->columns() != lazy.inputs.size())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "The lazy read of source {} does not read the columns it was chosen for", source);
        }

        const bool masks_stuffing = std::ranges::any_of(mask_sources, [&](const auto & mask_source) { return mask_source.second == source; });
        if (lazy_it == lazy_by_source.end() && !masks_stuffing)
            return plan;

        const auto index_name = makeRowIndexName(source);
        index_names.emplace(source, index_name);
        added_names.insert(index_name);

        /// Below a join that can leave the source unmatched the index is `Nullable`; every column of a source
        /// is gated alike.
        const auto & source_inputs = merged.sources[source].inputs;
        const bool nullable_index = !source_inputs.empty() && merged.getNearestStuffing(source_inputs.front()) != nullptr;

        auto above_read = makeRowIndexDAG(merge_tree, *plan.getCurrentHeader(), index_name, nullable_index);
        auto step = std::make_unique<ExpressionStep>(plan.getCurrentHeader(), std::move(above_read));
        step->setStepDescription("Row index");
        plan.addStep(std::move(step));
        return plan;
    }

    const MergedPlanDAG & merged;
    const LazyFrontier & frontier;
    const std::vector<bool> & lazy_sources;
    const std::unordered_map<const MergedPlanDAG::Stuffing *, size_t> & mask_sources;
    ContextPtr context;

    QueryPlan::Node * chain_top = nullptr;
    std::unordered_map<const QueryPlan::Node *, size_t> source_numbers;
    std::unordered_map<const QueryPlan::Node *, const QueryPlan::Node *> parents;
    NodeSet sort_key_nodes;
    std::map<size_t, LazySource> lazy_by_source;

    /// The values crossing the `LIMIT` besides the sort keys, as the columns they are carried in, and for
    /// each crossing value the column it is carried in.
    std::vector<const ActionsDAG::Node *> carried_values;
    std::unordered_map<const ActionsDAG::Node *, const ActionsDAG::Node *> carried_as;
    NodeSet needed;

    /// Per step, the nodes of its own DAG to output besides what it did, and the values they are.
    std::unordered_map<const QueryPlan::Node *, std::vector<std::pair<const ActionsDAG::Node *, const ActionsDAG::Node *>>> added_outputs;
    std::unordered_map<const QueryPlan::Node *, ActionsDAG::NodeRawConstPtrs> header_values;

    /// Per join, the carried values it passes through because a step below outputs them only now, and the
    /// outputs of its DAG that pass them.
    std::unordered_map<const QueryPlan::Node *, ActionsDAG::NodeRawConstPtrs> join_pass_through;
    std::unordered_map<const ActionsDAG::Node *, const ActionsDAG::Node *> join_passed_values;

    /// The row index of every source that has one, either for a lazy read or for a mask.
    std::map<size_t, String> index_names;
    NameSet added_names;
    std::unordered_map<const MergedPlanDAG::Stuffing *, size_t> mask_slots;
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

    rebuild.apply(query_plan, nodes, root, *sorting_node, sort_key_positions);
    return true;
}

}
