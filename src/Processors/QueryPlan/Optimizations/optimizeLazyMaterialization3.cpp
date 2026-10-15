#include <Processors/QueryPlan/Optimizations/Optimizations.h>

#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeFunction.h>
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
#include <Processors/QueryPlan/Optimizations/Utils.h>
#include <Processors/QueryPlan/Optimizations/actionsDAGUtils.h>
#include <Processors/QueryPlan/Optimizations/lazyFrontier.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/ReadFromObjectStorageStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/Transforms/LazyMaterializingTransform.h>
#include <Common/typeid_cast.h>

#include <map>
#include <ranges>
#include <set>
#include <unordered_set>

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

/// A copy of a value that crosses the `LIMIT` as a column, under a name of its own, which every step passes through.
constexpr std::string_view carried_column_prefix = "__lazy_carried_";

String makeRowIndexName(size_t source)
{
    return fmt::format("{}_{}", global_row_index_name, source);
}

String makeMaskName(size_t source)
{
    return fmt::format("{}{}", mask_column_prefix, source);
}

String makeCarriedName(size_t number)
{
    return fmt::format("{}{}", carried_column_prefix, number);
}

bool isNameOfThisPass(std::string_view name)
{
    return name.starts_with(global_row_index_name) || name.starts_with(mask_column_prefix) || name.starts_with(carried_column_prefix);
}

/// `if(mask, value, default)`: the value where the join matched the row, and the default where it padded
/// it, which is what the join itself stands there: the one `IDataType::insertDefaultInto` makes, which for some
/// types, such as `Date32`, is not the one `IDataType::getDefault` gives.
const ActionsDAG::Node & addMasked(
    ActionsDAG & dag, const ActionsDAG::Node & mask, const ActionsDAG::Node & value, const String & name, const ContextPtr & context)
{
    const auto & type = value.result_type;
    auto default_column = type->createColumn();
    type->insertDefaultInto(*default_column);
    const auto & default_value = dag.addColumn(
        ColumnConst::create(std::move(default_column), 1), type, fmt::format("defaultValueOfTypeName('{}')", type->getName()));

    auto if_function = FunctionFactory::instance().get("if", context);
    const auto & masked = dag.addFunction(if_function, {&mask, &value, &default_value}, {});
    if (masked.result_type->equals(*type))
        return dag.addAlias(masked, name);

    /// `if` works on full columns, so it drops `LowCardinality`, even nested, which the result above the `LIMIT` has to
    /// keep: the value had the same type below it.
    return dag.addCast(masked, type, name, context);
}

/// Whether a value is recomputed above the `LIMIT` where a join can pad it: a function or a constant of a side of
/// that join. A lambda is no value of a row: the higher-order function that reads it is masked instead.
bool isPaddedRecomputation(const MergedPlanDAG & merged, const LazyFrontier & frontier, const ActionsDAG::Node * node)
{
    return (node->type == ActionsDAG::ActionType::FUNCTION || node->type == ActionsDAG::ActionType::COLUMN)
        && !WhichDataType(node->result_type).isFunction() && isComputedAbove(frontier.at(node))
        && merged.getNearestPaddedSide(node) != nullptr;
}

/// Whether `reader` reads `value` as a join left it: the two are not under the same padded side, so `value` was computed
/// below that join and `reader` above it, or under another one.
bool readsAsJoinLeftIt(const MergedPlanDAG & merged, const ActionsDAG::Node * reader, const ActionsDAG::Node * value)
{
    return merged.getNearestPaddedSide(reader) != merged.getNearestPaddedSide(value);
}

/// The values recomputed above the `LIMIT` that are read as a join left them, see `readsAsJoinLeftIt`, or are outputs
/// of the subtree: those have a masked copy. An alias is whatever its child is, so the child of such an alias has one as
/// well. The values computed from a padded one under the same join read it unmasked, as they were computed from it
/// below the join, and a constant stays a constant for them.
NodeSet findMaskedValues(const MergedPlanDAG & merged, const LazyFrontier & frontier)
{
    NodeSet masked;
    std::vector<const ActionsDAG::Node *> stack(merged.getOutputs().begin(), merged.getOutputs().end());
    for (const auto & node : merged.getDAG().getNodes())
    {
        if (!isComputedAbove(frontier.at(&node)))
            continue;
        for (const auto * child : node.children)
            if (readsAsJoinLeftIt(merged, &node, child))
                stack.push_back(child);
    }

    while (!stack.empty())
    {
        const auto * node = stack.back();
        stack.pop_back();
        if (node->type == ActionsDAG::ActionType::INPUT || !isComputedAbove(frontier.at(node)) || !masked.insert(node).second)
            continue;
        if (node->type == ActionsDAG::ActionType::ALIAS)
            stack.push_back(node->children.front());
    }

    return masked;
}

/// The expression computed above the `LIMIT`: the outputs of `merged`, computed from the columns of
/// `header`. `positions` says which column holds a value that crossed or was read lazily, and `masks` which
/// one holds the mask of a padded side; every other value is recomputed, and a padded one of `masked` has a masked copy
/// as well, which the values reading it as the join left it read. The columns are bound by position, so names that repeat - two lazy reads of columns with the
/// same name - do not matter. Every column is consumed, so the result has exactly the header the subtree
/// had.
ActionsDAG buildAboveLimitDAG(
    const MergedPlanDAG & merged,
    const LazyFrontier & frontier,
    const std::unordered_map<const ActionsDAG::Node *, size_t> & positions,
    const std::unordered_map<const MergedPlanDAG::PaddedSide *, size_t> & masks,
    const NodeSet & masked,
    const Block & header,
    const ContextPtr & context)
{
    ActionsDAG dag(header.getColumnsWithTypeAndName());
    const auto inputs = dag.getInputs();
    std::unordered_map<const ActionsDAG::Node *, const ActionsDAG::Node *> copies;
    std::unordered_map<const ActionsDAG::Node *, const ActionsDAG::Node *> masked_copies;

    /// The copy of `value` that `reader` reads, or that the subtree outputs for a null `reader`.
    const auto copy_for = [&](const ActionsDAG::Node * reader, const ActionsDAG::Node * value)
    {
        if (!reader || readsAsJoinLeftIt(merged, reader, value))
            if (const auto it = masked_copies.find(value); it != masked_copies.end())
                return it->second;
        return copies.at(value);
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
                children.push_back(copy_for(node, child));

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

            if (masked.contains(node))
            {
                if (isPaddedRecomputation(merged, frontier, node))
                {
                    const auto * mask = inputs.at(masks.at(merged.getNearestPaddedSide(node)));
                    masked_copies.emplace(node, &addMasked(dag, *mask, *copy, node->result_name, context));
                }
                else if (node->type == ActionsDAG::ActionType::ALIAS)
                {
                    const auto * child = node->children.front();
                    if (const auto it = masked_copies.find(child); it != masked_copies.end())
                        masked_copies.emplace(node, &dag.addAlias(*it->second, node->result_name));
                }
            }

            copies.emplace(node, copy);
            stack.pop_back();
        }
    }

    auto & dag_outputs = dag.getOutputs();
    dag_outputs.clear();
    for (const auto * output : merged.getOutputs())
    {
        const auto * copy = copy_for(nullptr, output);
        if (copy->result_name != output->result_name)
            copy = &dag.addAlias(*copy, output->result_name);
        dag_outputs.push_back(copy);
    }

    return dag;
}

/// Whether a second, row-addressed read of this source is possible, which is what deferring a column of
/// it comes down to. The conditions the other lazy materialization path applies to its one source.
bool canReadLazily(const QueryPlan::Node & source, const QueryPlanOptimizationSettings & settings)
{
    auto * step = source.step.get();

    /// A MergeTree read is split by its pruning, which is not possible for every read: for a FINAL one that merges by
    /// every column it reads, or for `GraphiteMergeTree`.
    if (auto * merge_tree = typeid_cast<ReadFromMergeTree *>(step))
        return canUseLazyMaterializationForReadingStep(merge_tree) && merge_tree->canRemoveUnusedColumns();

    if (auto * object_storage = typeid_cast<ReadFromObjectStorageStep *>(step))
        return settings.optimize_lazy_materialization_for_object_storage && object_storage->canUseLazyMaterialization();

    return false;
}

/// A copy of the column at `position`, under `name`. The position is one among the columns the read had before this rebuild
/// added its own, which the read can have put anywhere.
struct ColumnCopy
{
    size_t position = 0;
    String name;
};

/// What this rebuild adds to a read: the row index, under `index_name` unless that is empty, then the `copies`, after
/// the other columns of `read_header`, in their order, so that the columns of the read keep their positions and the
/// added ones come after them. The columns at `index_positions` are what the index is computed from - for MergeTree the
/// offset of the part in the read and the offset of the row in its part, for an object storage read the index it
/// produces itself - and are consumed here unless the read had them before, which `consumed_positions` says. Below a
/// join that can leave the source unmatched the index is `Nullable`, so that the join stands it at NULL for the rows it
/// pads.
ActionsDAG makeAboveReadDAG(
    const Block & read_header,
    const std::vector<size_t> & index_positions,
    const std::vector<size_t> & consumed_positions,
    const String & index_name,
    bool nullable,
    const std::vector<ColumnCopy> & copies)
{
    ActionsDAG dag(read_header.getColumnsWithTypeAndName());
    const auto inputs = dag.getInputs();

    std::vector<bool> is_consumed(inputs.size(), false);
    for (size_t position : consumed_positions)
        is_consumed.at(position) = true;

    /// The columns the read had before come first, in their order, and only get appended to.
    auto & outputs = dag.getOutputs();
    outputs.clear();
    for (size_t position = 0; position < inputs.size(); ++position)
        if (!is_consumed[position])
            outputs.push_back(inputs[position]);
    const size_t kept_columns = outputs.size();

    if (!index_name.empty())
    {
        const ActionsDAG::Node * index = inputs.at(index_positions.front());
        if (index_positions.size() == 2)
        {
            auto plus = FunctionFactory::instance().get("plus", nullptr);
            index = &dag.addFunction(plus, {index, inputs.at(index_positions.back())}, {});
        }

        if (nullable)
        {
            auto to_nullable = FunctionFactory::instance().get("toNullable", nullptr);
            index = &dag.addFunction(to_nullable, {index}, {});
        }

        outputs.push_back(&dag.addAlias(*index, index_name));
    }

    for (const auto & copy : copies)
    {
        if (copy.position >= kept_columns)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Lazy materialization copies column {} of the {} columns of a read as {}", copy.position, kept_columns, copy.name);
        const auto * column = outputs[copy.position];
        outputs.push_back(&dag.addAlias(*column, copy.name));
    }

    return dag;
}

/// Makes `step_node` of an expression or a filter step the last column of its header, under `name`. This is for a value
/// the step computes without outputting it, such as `rowNumberInAllBlocks()` in `toString(rowNumberInAllBlocks())`
/// where only the outer function is recomputed above the `LIMIT`: no other step has the value to copy it from.
///
/// `ActionsDAG::updateHeader` puts the outputs of a DAG first and the columns the DAG does not read after them, so an
/// output appended as it is would come before those columns: their positions would move, which the steps above still
/// prune by, and a column of this rebuild would not be the last, which `prunedInputOf` requires. So the columns the
/// step passes through become outputs of its DAG first, in their order, which keeps the header as it was, and the new
/// output comes after them.
void appendOutputToStep(IQueryPlanStep & step, const ActionsDAG::Node & step_node, const String & name)
{
    ActionsDAG * dag = nullptr;
    if (auto * expression = typeid_cast<ExpressionStep *>(&step))
        dag = &expression->getExpression();
    else
        dag = &typeid_cast<FilterStep &>(step).getExpression();

    const auto input_header = step.getInputHeaders().front();
    const auto former_header = step.getOutputHeader();

    const auto header_columns = mapHeaderColumnsToInputs(dag->getInputs(), *input_header);
    auto & outputs = dag->getOutputs();
    for (size_t position = 0; position < header_columns.size(); ++position)
        if (header_columns.passesThrough(position))
            outputs.push_back(&dag->addInput(input_header->getByPosition(position)));
    outputs.push_back(&dag->addAlias(step_node, name));

    step.updateInputHeader(input_header);

    /// The steps above match the columns by position.
    const auto & header = *step.getOutputHeader();
    bool kept_columns = header.columns() == former_header->columns() + 1;
    for (size_t position = 0; kept_columns && position < former_header->columns(); ++position)
        kept_columns = header.getByPosition(position).name == former_header->getByPosition(position).name
            && header.getByPosition(position).type->equals(*former_header->getByPosition(position).type);
    if (!kept_columns)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Adding the output {} to {} changed its header from [{}] to [{}]",
            name, step.getName(), former_header->dumpStructure(), header.dumpStructure());
}

/// Rebuilds `Limit -> Sorting -> tree of Expression, Filter, BuildRuntimeFilter and JoinStepLogical ->
/// sources` so that the columns the frontier defers are read for the rows the `LIMIT` returns only.
///
/// The main branch is the original plan with fewer columns: every step is kept, with its own expressions
/// and names, and `removeUnusedColumns` takes away what nothing below the `LIMIT` needs any more, each
/// step once its children are built.
/// A value that crosses the `LIMIT` is copied where it is computed under a name of this rebuild, and every
/// step above passes the copy through, the joins because they are told to. Each lazily read source adds
/// its row index above the read, which goes up the same way, and above the `LIMIT` one `JoinLazyColumnsStep`
/// per such source reads its deferred columns by that index.
///
/// `prepare` checks everything without touching the plan and declines where the rebuild cannot carry the
/// frontier out; `apply` then changes the plan and does not decline any more.
class JoinPlanRebuild
{
public:
    /// `mask_sources` are the sources whose row index says whether the join of a padded side matched a row, for the
    /// padded sides the `masked_values` are on.
    JoinPlanRebuild(
        const MergedPlanDAG & merged_,
        const LazyFrontier & frontier_,
        const std::vector<bool> & lazy_sources_,
        const std::unordered_map<const MergedPlanDAG::PaddedSide *, size_t> & mask_sources_,
        const NodeSet & masked_values_,
        ContextPtr context_)
        : merged(merged_)
        , frontier(frontier_)
        , lazy_sources(lazy_sources_)
        , mask_sources(mask_sources_)
        , masked_values(masked_values_)
        , context(std::move(context_))
    {
        for (size_t source = 0; source < merged.sources.size(); ++source)
            source_numbers.emplace(merged.sources[source].plan_node, source);
    }

    bool prepare(QueryPlan::Node * chain_top_, const std::vector<size_t> & sort_key_positions)
    {
        chain_top = chain_top_;

        /// A subtree reading one source is what `optimizeLazyMaterialization2` handles. A join is the only step with more
        /// than one child the merged DAG goes through, so more sources than one mean a join.
        if (merged.sources.size() < 2)
            return false;

        const auto & outputs = merged.getOutputs();
        for (size_t position : sort_key_positions)
            sort_key_nodes.insert(outputs[position]);

        NodeSet kept_by_read;
        if (!chooseLazyReads(kept_by_read) || !keepColumnsOfUnreadJoinSides(kept_by_read))
            return false;

        /// The values that cross the `LIMIT`: what the frontier says, and the columns meant for a lazy read that the main
        /// read keeps anyway. A sort key crosses as a column the sorting needs. In the order of the merged DAG, so that the
        /// copies are numbered and cross the `LIMIT` the same way every time.
        std::vector<const ActionsDAG::Node *> carried_values;
        for (const auto & node : merged.getDAG().getNodes())
        {
            if (sort_key_nodes.contains(&node))
                continue;

            const auto it = frontier.placement.find(&node);
            if (kept_by_read.contains(&node) || (it != frontier.placement.end() && it->second == Placement::Carried))
                carried_values.push_back(&node);
        }

        /// Each value crosses as a copy of its own, made where it is computed: of the column the step there outputs, or of
        /// a value an expression or a filter computes without outputting it, which that step outputs then. Every step
        /// above passes the copy through.
        for (size_t number = 0; number < carried_values.size(); ++number)
        {
            const auto * value = carried_values[number];
            const auto & origin = merged.getOrigin(value);

            Carrying carrying{.value = value, .name = makeCarriedName(number)};
            const auto & values = merged.step_outputs.at(origin.plan_node);
            if (const auto it = std::ranges::find(values, value); it != values.end())
                carrying.position = it - values.begin();
            else if (origin.step_node
                && (typeid_cast<const ExpressionStep *>(origin.plan_node->step.get())
                    || typeid_cast<const FilterStep *>(origin.plan_node->step.get())))
                carrying.step_node = origin.step_node;
            else
                return false;

            added_names.insert(carrying.name);
            carried_copies.emplace_back(value, carrying.name);
            carryings[origin.plan_node].push_back(std::move(carrying));
        }

        return true;
    }

    /// `sorting_node` is the full sorting right below `root`, the `LIMIT`, or null without ORDER BY.
    void apply(QueryPlan & query_plan, QueryPlan::Nodes & nodes, QueryPlan::Node & root, QueryPlan::Node * sorting_node,
        const std::vector<size_t> & sort_key_positions)
    {
        /// An expression or a filter computing a carried value it does not output outputs a copy of it last. The steps
        /// above it still have its former header, which the copy comes after, as the other columns this rebuild adds.
        for (const auto & [node, node_carryings] : carryings)
            for (const auto & carrying : node_carryings)
                if (carrying.step_node)
                    appendOutputToStep(*node->step, *carrying.step_node, carrying.name);

        /// What the chain hands over: the sort keys and the copies of the values crossing the `LIMIT`. The header of the
        /// chain is the outputs of the merged DAG.
        std::vector<size_t> unneeded;
        auto next_sort_key = sort_key_positions.begin();
        for (size_t position = 0; position < merged.getOutputs().size(); ++position)
        {
            if (next_sort_key != sort_key_positions.end() && *next_sort_key == position)
                ++next_sort_key;
            else
                unneeded.push_back(position);
        }
        auto assembled = assemble(unneeded, nodes);
        auto & main_plan = assembled.plan;

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

            /// The chain keeps the columns of its former header it did not drop, in their order, so a sort key is found where
            /// it was, less the dropped positions before it. Both are in order, so they are followed at once. A copy of a
            /// carried value and a row index have names of their own.
            const auto & dropped = assembled.dropped_positions;
            auto next_dropped = dropped.begin();
            auto & projection_outputs = projection.getOutputs();
            projection_outputs.clear();
            for (size_t position : sort_key_positions)
            {
                while (next_dropped != dropped.end() && *next_dropped < position)
                    ++next_dropped;
                if (next_dropped != dropped.end() && *next_dropped == position)
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Lazy materialization lost the sort key {} below the LIMIT", outputs[position]->result_name);
                const auto dropped_before = next_dropped - dropped.begin();
                projection_outputs.push_back(inputs.at(position - dropped_before));
                slots.push_back(outputs[position]);
                slot_indexes.emplace_back();
            }
            for (const auto & [value, name] : carried_copies)
            {
                projection_outputs.push_back(inputs.at(header.getPositionByName(name)));
                slots.push_back(value);
                slot_indexes.emplace_back();
            }
            for (const auto & [source, index_name] : index_names)
            {
                projection_outputs.push_back(inputs[header.getPositionByName(index_name)]);
                slots.push_back(nullptr);
                slot_indexes.push_back(index_name);
            }

            auto step = std::make_unique<ExpressionStep>(main_plan.getCurrentHeader(), std::move(projection));
            step->setStepDescription("Columns crossing the LIMIT");
            main_plan.addStep(std::move(step));
        }

        if (sorting_node)
        {
            auto sorting_step = std::move(sorting_node->step);
            sorting_step->updateInputHeader(main_plan.getCurrentHeader());
            main_plan.addStep(std::move(sorting_step));
        }

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

            /// A mask column serves every padded side its source masks.
            std::map<size_t, size_t> mask_slot_of_source;
            for (const auto & [padded_side, source] : mask_sources)
            {
                auto [it, inserted] = mask_slot_of_source.emplace(source, slots.size());
                if (inserted)
                {
                    const auto * index = masks_dag.getInputs()[header.getPositionByName(index_names.at(source))];
                    masks_dag.getOutputs().push_back(&masks_dag.addFunction(is_not_null, {index}, makeMaskName(source)));
                    slots.push_back(nullptr);
                    slot_indexes.emplace_back();
                }
                mask_slots.emplace(padded_side, it->second);
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
            for (auto & [padded_side, slot] : mask_slots)
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

            auto above = buildAboveLimitDAG(merged, frontier, positions, mask_slots, masked_values, header, context);

            auto step = std::make_unique<ExpressionStep>(main_plan.getCurrentHeader(), std::move(above));
            step->setStepDescription("Computed after the LIMIT");
            main_plan.addStep(std::move(step));
        }

        query_plan.replaceNodeWithPlan(&root, std::move(main_plan), std::move(expected_header));
    }

private:
    struct LazySource
    {
        /// The source columns the main read keeps, by name, for an object storage read; the positions of the ones the lazy
        /// read fetches, for a MergeTree read; and the inputs standing for them.
        NameSet eager_names;
        std::vector<size_t> positions;
        std::vector<const ActionsDAG::Node *> inputs;

        /// Set by `apply`.
        std::unique_ptr<LazilyReadFromMergeTree> merge_tree_reading;
        std::unique_ptr<LazilyReadFromObjectStorage> object_storage_reading;
        RangesInDataParts parts;
        LazyObjectStorageFileRegistryPtr row_index_registry;
    };

    /// The nodes between `chain_top` and the sources, the ones the walk building the merged DAG entered, top-down, the
    /// sources included, with the right subtree of a node before the left one, so that in reverse they come bottom-up,
    /// left to right.
    std::vector<QueryPlan::Node *> collectNodes() const
    {
        std::vector<QueryPlan::Node *> top_down_nodes;
        std::vector<QueryPlan::Node *> stack{chain_top};
        while (!stack.empty())
        {
            auto * node = stack.back();
            stack.pop_back();
            top_down_nodes.push_back(node);

            if (source_numbers.contains(node))
                continue;

            for (auto * child : node->children)
                stack.push_back(child);
        }
        return top_down_nodes;
    }

    /// Picks the columns each source reads lazily. A column the frontier wants read lazily that something
    /// below the `LIMIT` reads as well, or that the read keeps anyway, stays in the main read and crosses the
    /// `LIMIT` as a column instead; those are returned in `kept_by_read`.
    bool chooseLazyReads(NodeSet & kept_by_read)
    {
        for (size_t source = 0; source < merged.sources.size(); ++source)
        {
            if (!lazy_sources[source])
                continue;

            LazySource lazy;
            const auto * step = merged.sources[source].plan_node->step.get();

            if (const auto * merge_tree = typeid_cast<const ReadFromMergeTree *>(step))
            {
                /// The positions the main read need not produce: nothing below the `LIMIT` reads them, and they do not
                /// cross it - the ones meant for the lazy read, and the ones nothing needs at all. The latter matter as
                /// well: the main read is pruned of them anyway, and a column of storage read for several positions is
                /// left out of the read only if none of them is needed. The inputs of a source follow its header.
                const auto & inputs = merged.sources[source].inputs;
                std::vector<size_t> unneeded;
                for (size_t position = 0; position < inputs.size(); ++position)
                {
                    if (!isComputedBelow(frontier.at(inputs[position])))
                        unneeded.push_back(position);
                }

                /// A column meant for the lazy read that the main read reads anyway, such as one FINAL merges by or
                /// PREWHERE reads, crosses the `LIMIT` instead. Those the read can leave out are some of the unneeded
                /// ones, in their order, so both lists are followed at once.
                const auto readable = merge_tree->getLazilyReadablePositions(unneeded);
                auto next_readable = readable.begin();
                for (size_t position = 0; position < inputs.size(); ++position)
                {
                    const bool is_readable = next_readable != readable.end() && *next_readable == position;
                    if (is_readable)
                        ++next_readable;

                    const auto * input = inputs[position];
                    if (!isComputedAbove(frontier.at(input)))
                        continue;

                    if (is_readable)
                    {
                        lazy.positions.push_back(position);
                        lazy.inputs.push_back(input);
                    }
                    else
                        kept_by_read.insert(input);
                }

                if (!lazy.inputs.empty())
                    lazy_by_source.emplace(source, std::move(lazy));
                continue;
            }

            std::vector<const ActionsDAG::Node *> candidates;
            for (const auto * input : merged.sources[source].inputs)
            {
                const auto placed = frontier.at(input);
                if (placed == Placement::Deferred)
                    candidates.push_back(input);
                else
                {
                    lazy.eager_names.insert(input->result_name);
                    if (placed == Placement::Recomputed)
                        kept_by_read.insert(input);
                }
            }

            if (candidates.empty())
                continue;

            lazy.inputs = std::move(candidates);
            if (!settleObjectStorageLazyRead(source, lazy, kept_by_read))
                return false;

            if (!lazy.inputs.empty())
                lazy_by_source.emplace(source, std::move(lazy));
        }

        return !lazy_by_source.empty();
    }

    /// Leaves to the lazy read of an object storage source those of `lazy.inputs` the read can leave out given the columns
    /// it keeps, `lazy.eager_names`, and keeps the others, which cross the `LIMIT` instead. Keeping a column can keep
    /// others with it - a column whose `DEFAULT` is computed from it - so this goes on until nothing more is kept.
    bool settleObjectStorageLazyRead(size_t source, LazySource & lazy, NodeSet & kept_by_read) const
    {
        const auto & step = typeid_cast<const ReadFromObjectStorageStep &>(*merged.sources[source].plan_node->step);
        while (true)
        {
            const auto lazily_read = step.getLazilyReadColumns(lazy.eager_names);
            const auto kept = std::erase_if(lazy.inputs, [&](const ActionsDAG::Node * input)
            {
                if (lazily_read.contains(input->result_name))
                    return false;
                lazy.eager_names.insert(input->result_name);
                kept_by_read.insert(input);
                return true;
            });

            /// The lazy read fetches every column the main read leaves out, so the two have to agree.
            if (kept == 0)
                return lazy.inputs.size() == lazily_read.size();
        }
    }

    /// A join keeps the first input of a side no condition of it reads when nothing else of that side is needed below the
    /// `LIMIT`, for the number of rows, so that input is computed below the `LIMIT`, and the columns of the reads it is
    /// computed from have to stay in the main read: those meant for a lazy read cross the `LIMIT` instead. The input can
    /// be a column of a read, under whatever name the steps between give it, a value computed from columns of one read
    /// or of several, or a constant, which reads none.
    bool keepColumnsOfUnreadJoinSides(NodeSet & kept_by_read)
    {
        /// The object storage sources whose lazy read lost a column, which may take others with it.
        std::set<size_t> changed_object_storage_sources;

        for (const auto & side_inputs : merged.unread_join_sides)
        {
            bool is_any_needed_below = false;
            for (const auto * input : side_inputs)
            {
                if (isComputedBelow(frontier.at(input)) || kept_by_read.contains(input))
                {
                    is_any_needed_below = true;
                    break;
                }
            }

            if (is_any_needed_below)
                continue;

            for (const auto * column : findReachableNodes({side_inputs.front()}))
            {
                if (column->type != ActionsDAG::ActionType::INPUT)
                    continue;

                /// An input of the merged DAG is a column of exactly one read.
                const auto & column_sources = merged.getSources(column);
                if (column_sources.count() != 1)
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Input {} of the merged plan DAG reads {} sources", column->result_name, column_sources.count());
                const auto source = *column_sources.begin();
                const auto lazy_it = lazy_by_source.find(source);
                if (lazy_it == lazy_by_source.end())
                    continue;

                /// A column the read does not leave to its lazy read is in the main read already.
                auto & lazy = lazy_it->second;
                const auto input_it = std::ranges::find(lazy.inputs, column);
                if (input_it == lazy.inputs.end())
                    continue;

                const auto index = input_it - lazy.inputs.begin();
                lazy.inputs.erase(input_it);
                if (!lazy.positions.empty())
                    lazy.positions.erase(lazy.positions.begin() + index);
                else
                {
                    lazy.eager_names.insert(column->result_name);
                    changed_object_storage_sources.insert(source);
                }
                kept_by_read.insert(column);

                if (lazy.inputs.empty())
                    lazy_by_source.erase(lazy_it);
            }
        }

        for (size_t source : changed_object_storage_sources)
        {
            const auto lazy_it = lazy_by_source.find(source);
            if (lazy_it == lazy_by_source.end())
                continue;

            if (!settleObjectStorageLazyRead(source, lazy_it->second, kept_by_read))
                return false;

            if (lazy_it->second.inputs.empty())
                lazy_by_source.erase(lazy_it);
        }

        return !lazy_by_source.empty();
    }

    bool isAddedColumn(const String & name) const { return added_names.contains(name); }

    /// A rebuilt node: its plan, and the positions of its former header it no longer has. Its header is the former one
    /// without those positions, in the same order, then what its step appended, then the columns this rebuild added.
    struct Assembled
    {
        QueryPlan plan;
        std::vector<size_t> dropped_positions;
    };

    /// A rebuilt child as the step above it sees it: everything but the columns this rebuild added, which the step passes
    /// through once it is pruned. Those come last, and have names of their own.
    IQueryPlanStep::PrunedInput prunedInputOf(const Assembled & child, size_t former_columns) const
    {
        const auto & header = *child.plan.getCurrentHeader();
        size_t columns = header.columns();
        while (columns > 0 && isAddedColumn(header.getByPosition(columns - 1).name))
            --columns;

        for (size_t position = 0; position < columns; ++position)
            if (isAddedColumn(header.getByPosition(position).name))
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Lazy materialization added column {} before the end of [{}]", header.getByPosition(position).name, header.dumpNames());

        if (columns < former_columns - child.dropped_positions.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Lazy materialization lost columns: [{}] has {} of the {} columns that remain of {}",
                header.dumpNames(), columns, former_columns - child.dropped_positions.size(), former_columns);

        Block pruned_header;
        for (size_t position = 0; position < columns; ++position)
            pruned_header.insert(header.getByPosition(position));

        return {child.dropped_positions, std::make_shared<const Block>(std::move(pruned_header))};
    }

    /// Builds the main branch: prunes from each step what the step above does not need of it, top-down, and
    /// assembles it bottom-up - splits the lazily read columns off the reads, adds the row indexes, which
    /// the joins pass through, and prunes a step once its children are built, with their real headers.
    /// `unneeded` is what is not needed of `chain_top`.
    Assembled assemble(const std::vector<size_t> & unneeded, QueryPlan::Nodes & nodes)
    {
        const auto top_down_nodes = collectNodes();

        /// A node a value is carried from keeps the column it is copied from.
        std::unordered_map<const QueryPlan::Node *, std::vector<size_t>> unneeded_of{{chain_top, unneeded}};
        for (auto * node : top_down_nodes)
        {
            auto & node_unneeded = unneeded_of.at(node);
            if (const auto it = carryings.find(node); it != carryings.end())
                for (const auto & carrying : it->second)
                    if (!carrying.step_node)
                        std::erase(node_unneeded, carrying.position);

            if (source_numbers.contains(node))
                continue;

            auto child_unneeded = node->step->getUnneededColumns(node_unneeded);
            for (size_t child = 0; child < node->children.size(); ++child)
                unneeded_of[node->children[child]] = std::move(child_unneeded.at(child));
        }

        std::unordered_map<const QueryPlan::Node *, Assembled> assembled;
        for (auto * node : top_down_nodes | std::views::reverse)
        {
            const auto & node_unneeded = unneeded_of.at(node);
            if (const auto it = source_numbers.find(node); it != source_numbers.end())
            {
                assembled.emplace(node, assembleSource(node, it->second, node_unneeded, nodes));
                continue;
            }

            std::vector<Assembled> children;
            for (auto * child : node->children)
            {
                auto child_it = assembled.find(child);
                children.push_back(std::move(child_it->second));
                assembled.erase(child_it);
            }

            auto result = assembleStep(node, node_unneeded, children);
            addCopiesOfCarriedColumns(node, result);
            assembled.emplace(node, std::move(result));
        }

        return std::move(assembled.at(chain_top));
    }

    /// The copies of the values carried from the columns `node` outputs, positions of its former header, which the steps
    /// above pass through. A read makes them with its row index.
    std::vector<ColumnCopy> copiesOfCarriedColumns(const QueryPlan::Node * node, const std::vector<size_t> & dropped_positions) const
    {
        std::vector<ColumnCopy> copies;
        const auto it = carryings.find(node);
        if (it == carryings.end())
            return copies;

        for (const auto & carrying : it->second)
        {
            if (carrying.step_node)
                continue;

            const auto first_not_before = std::ranges::lower_bound(dropped_positions, carrying.position);
            if (first_not_before != dropped_positions.end() && *first_not_before == carrying.position)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Lazy materialization lost the carried value {} of {}", carrying.value->result_name, node->step->getName());
            copies.push_back({carrying.position - (first_not_before - dropped_positions.begin()), carrying.name});
        }
        return copies;
    }

    void addCopiesOfCarriedColumns(const QueryPlan::Node * node, Assembled & assembled)
    {
        auto copies = copiesOfCarriedColumns(node, assembled.dropped_positions);
        if (copies.empty())
            return;

        auto dag = makeAboveReadDAG(*assembled.plan.getCurrentHeader(), {}, {}, {}, false, copies);
        auto step = std::make_unique<ExpressionStep>(assembled.plan.getCurrentHeader(), std::move(dag));
        step->setStepDescription("Columns carried across the LIMIT");
        steps_above_child.insert(step.get());
        assembled.plan.addStep(std::move(step));
    }

    /// Prunes and assembles the step of a node on top of its assembled children.
    Assembled assembleStep(QueryPlan::Node * node, const std::vector<size_t> & unneeded, std::vector<Assembled> & children)
    {
        auto * step = node->step.get();

        Assembled result;
        std::vector<IQueryPlanStep::PrunedInput> pruned;
        for (size_t child = 0; child < children.size(); ++child)
            pruned.push_back(prunedInputOf(children[child], step->getInputHeaders()[child]->columns()));
        result.dropped_positions = step->removeUnusedColumns(unneeded, pruned).dropped_output_positions;

        /// The columns this rebuild added below pass through: a join is told to, the other steps do by themselves,
        /// after their own columns, since those columns come last in their input.
        if (auto * join = typeid_cast<JoinStepLogical *>(step))
        {
            for (size_t side = 0; side < 2; ++side)
                for (const auto & column : *children[side].plan.getCurrentHeader())
                    if (isAddedColumn(column.name))
                        join->addPassThroughColumn(column, side == 0 ? JoinTableSide::Left : JoinTableSide::Right);

            join->updateInputHeaders({children[0].plan.getCurrentHeader(), children[1].plan.getCurrentHeader()});
        }
        else
            step->updateInputHeader(children.front().plan.getCurrentHeader());

        if (children.size() == 1)
        {
            result.plan = std::move(children.front().plan);
            const bool is_above_row_index = steps_above_child.contains(result.plan.getRootNode()->step.get());
            result.plan.addStep(std::move(node->step));
            if (is_above_row_index)
                mergeRowIndexIntoParent(*result.plan.getRootNode());
        }
        else
        {
            std::vector<QueryPlanPtr> plans;
            for (auto & child : children)
                plans.emplace_back(std::make_unique<QueryPlan>(std::move(child.plan)));
            result.plan.unitePlans(std::move(node->step), std::move(plans));
        }
        return result;
    }

    Assembled assembleSource(QueryPlan::Node * node, size_t source, const std::vector<size_t> & unneeded, QueryPlan::Nodes & nodes)
    {
        /// Whatever is below the source stays as it is.
        Assembled result;
        auto & plan = result.plan;
        plan = QueryPlan::extractSubplan(node, nodes);

        auto * merge_tree = typeid_cast<ReadFromMergeTree *>(plan.getRootNode()->step.get());
        auto * object_storage = typeid_cast<ReadFromObjectStorageStep *>(plan.getRootNode()->step.get());

        const auto lazy_it = lazy_by_source.find(source);
        auto * lazy = lazy_it != lazy_by_source.end() ? &lazy_it->second : nullptr;
        const Block * lazy_header = nullptr;

        if (merge_tree && merge_tree->canRemoveUnusedColumns())
        {
            /// The read drops what nothing below the `LIMIT` needs, and hands the columns read lazily to a lazy read.
            static const std::vector<size_t> no_positions;
            const auto & lazy_positions = lazy ? lazy->positions : no_positions;
            /// Both in order, so they are followed at once.
            auto next_unneeded = unneeded.begin();
            for (size_t position : lazy_positions)
            {
                while (next_unneeded != unneeded.end() && *next_unneeded < position)
                    ++next_unneeded;
                if (next_unneeded == unneeded.end() || *next_unneeded != position)
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Column {} of source {} is read lazily, but needed below the LIMIT",
                        plan.getCurrentHeader()->getByPosition(position).name, source);
            }

            auto [pruned, lazy_reading] = merge_tree->removeUnusedColumnsAndCreateLazyReadStep(unneeded, lazy_positions);
            result.dropped_positions = std::move(pruned.dropped_output_positions);

            if (lazy)
            {
                lazy->merge_tree_reading = std::move(lazy_reading);
                if (lazy->merge_tree_reading)
                    lazy_header = lazy->merge_tree_reading->getOutputHeader().get();

                /// The lazy read fetches exactly the rows the main read selected, addressed by their row
                /// index. It must not apply the vector search rescoring filter again: that one belongs to
                /// the main read, and applying it against the candidates of the vector index can drop a
                /// requested row.
                lazy->parts = merge_tree->getParts();
                for (auto & part : lazy->parts)
                    part.read_hints.use_vector_search_result_filter = false;
            }
        }
        else if (lazy)
        {
            /// The columns of a read are storage columns, with unique names.
            const Block former_header = *plan.getCurrentHeader();
            lazy->object_storage_reading = object_storage->keepOnlyRequiredColumnsAndCreateLazyReadStep(lazy->eager_names);
            if (lazy->object_storage_reading)
                lazy_header = lazy->object_storage_reading->getOutputHeader().get();
            lazy->row_index_registry = object_storage->getLazyRowIndexRegistry();

            const auto & read_header = *plan.getCurrentHeader();
            for (size_t position = 0; position < former_header.columns(); ++position)
                if (!read_header.has(former_header.getByPosition(position).name))
                    result.dropped_positions.push_back(position);
        }

        if (lazy && (!lazy_header || lazy_header->columns() != lazy->inputs.size()))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The lazy read of source {} does not read the columns it was chosen for", source);

        const auto copies = copiesOfCarriedColumns(node, result.dropped_positions);
        const bool masks_padded_side = std::ranges::any_of(mask_sources, [&](const auto & mask_source) { return mask_source.second == source; });
        if (!lazy && !masks_padded_side)
        {
            if (!copies.empty())
            {
                auto above_read = makeAboveReadDAG(*plan.getCurrentHeader(), {}, {}, {}, false, copies);
                auto step = std::make_unique<ExpressionStep>(plan.getCurrentHeader(), std::move(above_read));
                step->setStepDescription("Columns carried across the LIMIT");
                steps_above_child.insert(step.get());
                plan.addStep(std::move(step));
            }
            return result;
        }

        /// The columns the row index is computed from, and the ones of them the read did not produce before.
        std::vector<size_t> index_positions;
        std::vector<size_t> consumed_positions;
        if (merge_tree)
        {
            const auto offsets = merge_tree->addStartingPartOffsetAndPartOffset();
            index_positions = {offsets.part_starting_offset_position, offsets.part_offset_position};
            if (offsets.added_part_starting_offset)
                consumed_positions.push_back(offsets.part_starting_offset_position);
            if (offsets.added_part_offset)
                consumed_positions.push_back(offsets.part_offset_position);
        }
        else
        {
            /// The lazy split appends the index the read produces; the name is one this pass reserves.
            const auto position = plan.getCurrentHeader()->getPositionByName(String(global_row_index_name));
            index_positions = {position};
            consumed_positions = {position};
        }

        const auto index_name = makeRowIndexName(source);
        index_names.emplace(source, index_name);
        added_names.insert(index_name);

        /// Below a join that can leave the source unmatched the index is `Nullable`; every column of a source
        /// is gated alike.
        const auto & source_inputs = merged.sources[source].inputs;
        const bool nullable_index = !source_inputs.empty() && merged.getNearestPaddedSide(source_inputs.front()) != nullptr;

        auto above_read = makeAboveReadDAG(*plan.getCurrentHeader(), index_positions, consumed_positions, index_name, nullable_index, copies);
        auto step = std::make_unique<ExpressionStep>(plan.getCurrentHeader(), std::move(above_read));
        step->setStepDescription("Row index");
        steps_above_child.insert(step.get());
        plan.addStep(std::move(step));
        return result;
    }

    /// An expression or a filter right above the read computes the row index itself, so that a filter stays right
    /// above the read, where PREWHERE optimization and lazy FINAL look for it. Other steps keep the row index step
    /// below them. The step keeps its own description: the row index is a detail of this rebuild.
    static void mergeRowIndexIntoParent(QueryPlan::Node & parent)
    {
        if (!typeid_cast<ExpressionStep *>(parent.step.get()) && !typeid_cast<FilterStep *>(parent.step.get()))
            return;

        String description(parent.step->getStepDescription());
        const auto header = parent.step->getOutputHeader();
        QueryPlan::Nodes unused_nodes;
        if (!tryMergeExpressions(&parent, unused_nodes, {}))
            return;

        /// The steps above match the columns by position.
        if (!blocksHaveEqualStructure(*header, *parent.step->getOutputHeader()))
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Merging the row index changed the header of {} from [{}] to [{}]",
                parent.step->getName(), header->dumpStructure(), parent.step->getOutputHeader()->dumpStructure());

        const auto description_size = description.size();
        parent.step->setStepDescription(std::move(description), description_size);
    }

    const MergedPlanDAG & merged;
    const LazyFrontier & frontier;
    const std::vector<bool> & lazy_sources;
    const std::unordered_map<const MergedPlanDAG::PaddedSide *, size_t> & mask_sources;
    const NodeSet & masked_values;
    ContextPtr context;

    QueryPlan::Node * chain_top = nullptr;
    std::unordered_map<const QueryPlan::Node *, size_t> source_numbers;
    NodeSet sort_key_nodes;
    std::map<size_t, LazySource> lazy_by_source;
    /// The steps this rebuild adds right above a step - the row index and the copies of the carried values - to be
    /// merged into the step above when that can take it.
    std::unordered_set<const IQueryPlanStep *> steps_above_child;

    /// A value crossing the `LIMIT` besides the sort keys, carried from a node as a copy under `name`: of the column at
    /// `position` of its header, or, without such a column, of `step_node` of its step, which outputs it last.
    struct Carrying
    {
        const ActionsDAG::Node * value = nullptr;
        String name;
        size_t position = 0;
        const ActionsDAG::Node * step_node = nullptr;
    };

    /// By the node each value is carried from, in the order of the values.
    std::unordered_map<const QueryPlan::Node *, std::vector<Carrying>> carryings;
    /// Each carried value and the name of its copy, in the order of the values, which is the order the copies cross the
    /// `LIMIT` in: the same every time, unlike the order of the nodes in `carryings`.
    std::vector<std::pair<const ActionsDAG::Node *, String>> carried_copies;

    /// The row index of every source that has one, either for a lazy read or for a mask.
    std::map<size_t, String> index_names;
    NameSet added_names;
    std::unordered_map<const MergedPlanDAG::PaddedSide *, size_t> mask_slots;
};

/// A cheap look at the plan before the merged DAG is built: the steps its walk can go through hold a join, and a source
/// they reach can be read lazily. The walk can stop higher up, so this only rules plans out. The columns this pass adds
/// are found by name, so a name of this pass in a header of those steps or sources rules the plan out as well.
bool mayDeferOverJoins(const QueryPlan::Node & chain_top, const QueryPlanOptimizationSettings & settings)
{
    bool has_join = false;
    bool has_lazy_source = false;

    std::vector<const QueryPlan::Node *> stack{&chain_top};
    while (!stack.empty())
    {
        const auto * node = stack.back();
        stack.pop_back();

        for (const auto & column : *node->step->getOutputHeader())
            if (isNameOfThisPass(column.name))
                return false;

        if (!canEnterStep(*node))
        {
            has_lazy_source |= canReadLazily(*node, settings);
            continue;
        }

        has_join |= typeid_cast<const JoinStepLogical *>(node->step.get()) != nullptr;
        stack.insert(stack.end(), node->children.begin(), node->children.end());
    }

    return has_join && has_lazy_source;
}

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

    /// With ORDER BY, the sort keys are needed below the `LIMIT`. This runs before reading in order is applied,
    /// so a sorting is a full one. Without ORDER BY, the `LIMIT` stops the reads early, but only after whole
    /// blocks in every stream, and a hash join reads its build side completely, so the reads still go through
    /// far more rows than the `LIMIT` keeps, and deferring their columns pays off the same way.
    QueryPlan::Node * sorting_node = nullptr;
    SortingStep * sorting_step = nullptr;
    if (auto * sorting = typeid_cast<SortingStep *>(root.children.front()->step.get()))
    {
        if (sorting->getType() != SortingStep::Type::Full)
            return false;

        sorting_node = root.children.front();
        sorting_step = sorting;
    }

    auto * chain_top = sorting_node ? sorting_node->children.front() : root.children.front();
    if (!mayDeferOverJoins(*chain_top, settings))
        return false;

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
    for (const auto & description : sorting_step ? sorting_step->getSortDescription() : SortDescription{})
    {
        const auto it = std::ranges::find_if(outputs, [&](const auto * output) { return output->result_name == description.column_name; });
        if (it == outputs.end())
            return false;
        sort_key_positions.push_back(it - outputs.begin());
    }
    std::ranges::sort(sort_key_positions);
    sort_key_positions.erase(std::unique(sort_key_positions.begin(), sort_key_positions.end()), sort_key_positions.end());

    /// A padded side can be masked by the row index of a MergeTree source on it, with no other padded side in between:
    /// that index is NULL exactly at the rows its join, or a join above it, left unmatched.
    std::unordered_map<const MergedPlanDAG::PaddedSide *, size_t> mask_candidates;
    ContextPtr context;
    for (size_t source = 0; source < merged.sources.size(); ++source)
    {
        const auto & inputs = merged.sources[source].inputs;
        auto * merge_tree = typeid_cast<ReadFromMergeTree *>(merged.sources[source].plan_node->step.get());
        if (!lazy_sources[source] || !merge_tree || inputs.empty())
            continue;

        if (const auto * padded_side = merged.getNearestPaddedSide(inputs.front()))
        {
            mask_candidates.emplace(padded_side, source);
            context = merge_tree->getContext();
        }
    }

    std::unordered_set<const MergedPlanDAG::PaddedSide *> masked_padded_sides;
    for (const auto & [padded_side, source] : mask_candidates)
        masked_padded_sides.insert(padded_side);

    const auto frontier = chooseLazyFrontier(merged, sort_key_positions, lazy_sources, masked_padded_sides);
    if (!frontier.defersAnything())
        return false;

    /// Only the masks the masked values use are computed.
    const auto masked_values = findMaskedValues(merged, frontier);
    std::unordered_map<const MergedPlanDAG::PaddedSide *, size_t> mask_sources;
    for (const auto * node : masked_values)
        if (isPaddedRecomputation(merged, frontier, node))
            mask_sources.emplace(merged.getNearestPaddedSide(node), mask_candidates.at(merged.getNearestPaddedSide(node)));

    JoinPlanRebuild rebuild(merged, frontier, lazy_sources, mask_sources, masked_values, context);
    if (!rebuild.prepare(chain_top, sort_key_positions))
        return false;

    rebuild.apply(query_plan, nodes, root, sorting_node, sort_key_positions);
    return true;
}

}
