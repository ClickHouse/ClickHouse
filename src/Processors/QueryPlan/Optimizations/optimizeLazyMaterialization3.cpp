#include <Processors/QueryPlan/Optimizations/Optimizations.h>

#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeFunction.h>
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
#include <Processors/QueryPlan/ReadFromRemote.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/TotalsHavingStep.h>
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
    /// `chain_top` is the node right below the `LIMIT`, or below the sorting, and `sort_key_positions` are the positions of
    /// its header the sorting reads. `mask_sources` are the sources whose row index says whether the join of a padded
    /// side matched a row, for the padded sides the `masked_values` are on.
    JoinPlanRebuild(
        const MergedPlanDAG & merged_,
        const LazyFrontier & frontier_,
        const std::vector<bool> & lazy_sources_,
        const std::unordered_map<const MergedPlanDAG::PaddedSide *, size_t> & mask_sources_,
        const NodeSet & masked_values_,
        ContextPtr context_,
        QueryPlan::Node * chain_top_,
        const std::vector<size_t> & sort_key_positions_)
        : merged(merged_)
        , frontier(frontier_)
        , lazy_sources(lazy_sources_)
        , mask_sources(mask_sources_)
        , masked_values(masked_values_)
        , context(std::move(context_))
        , chain_top(chain_top_)
        , sort_key_positions(sort_key_positions_)
    {
        for (size_t source = 0; source < merged.sources.size(); ++source)
            source_numbers.emplace(merged.sources[source].plan_node, source);
        for (const auto & [padded_side, source] : mask_sources)
            sources_with_mask.insert(source);
    }

    bool prepare()
    {
        NodeSet sort_key_nodes;
        const auto & outputs = merged.getOutputs();
        for (size_t position : sort_key_positions)
            sort_key_nodes.insert(outputs[position]);

        NodeSet kept_by_read;
        if (!chooseLazyReads(kept_by_read))
            return false;

        /// The pruning of the plan can keep a column meant for a lazy read: a join keeps a column of a side it reads nothing
        /// of, for the number of rows, and that column can be, or be computed from, a column of a read. Such a column stays in
        /// the main read and crosses the `LIMIT` instead. One pass is enough: the copy of such a column is made at its own
        /// read, which only keeps the column it already keeps, and changes nothing above the read or at another read.
        if (!collectCarriedValues(sort_key_nodes, kept_by_read))
            return false;

        if (!keepColumnsThePruningKeeps(findUnneededColumns(collectNodes()), kept_by_read))
            return false;

        if (!collectCarriedValues(sort_key_nodes, kept_by_read))
            return false;

        return !lazy_by_source.empty();
    }

    /// The values that cross the `LIMIT`: what the frontier says, and the columns meant for a lazy read that the main read
    /// keeps anyway. A sort key crosses as a column the sorting needs. In the order of the merged DAG, so that the copies
    /// are numbered and cross the `LIMIT` the same way every time.
    ///
    /// Each value crosses as a copy of its own, made where it is computed: of the column the step there outputs, or of a
    /// value an expression or a filter computes without outputting it, which that step outputs then. Every step above
    /// passes the copy through.
    bool collectCarriedValues(const NodeSet & sort_key_nodes, const NodeSet & kept_by_read)
    {
        for (const auto & carrying : carried)
            added_names.erase(carrying.name);
        carried.clear();
        carried_from.clear();

        for (const auto & node : merged.getDAG().getNodes())
        {
            const auto * value = &node;
            const auto it = frontier.placement.find(value);
            const bool is_carried = kept_by_read.contains(value) || (it != frontier.placement.end() && it->second == Placement::Carried);
            if (!is_carried || sort_key_nodes.contains(value))
                continue;

            const auto & origin = merged.getOrigin(value);
            Carrying carrying{.value = value, .name = makeCarriedName(carried.size())};
            const auto & values = merged.step_outputs.at(origin.plan_node);
            if (const auto output_it = std::ranges::find(values, value); output_it != values.end())
                carrying.position = output_it - values.begin();
            else if (origin.step_node
                && (typeid_cast<const ExpressionStep *>(origin.plan_node->step.get())
                    || typeid_cast<const FilterStep *>(origin.plan_node->step.get())))
                carrying.step_node = origin.step_node;
            else
                return false;

            added_names.insert(carrying.name);
            carried_from[origin.plan_node].push_back(carried.size());
            carried.push_back(std::move(carrying));
        }

        return true;
    }

    /// Moves to the main read the columns meant for a lazy read that the pruning of the plan keeps, `unneeded_of` says
    /// which; each read on its own. Returns false where an object storage read cannot be split for the rest.
    bool keepColumnsThePruningKeeps(
        const std::unordered_map<const QueryPlan::Node *, std::vector<size_t>> & unneeded_of, NodeSet & kept_by_read)
    {
        for (auto it = lazy_by_source.begin(); it != lazy_by_source.end();)
        {
            const auto source = it->first;
            auto & lazy = it->second;
            const auto & unneeded = unneeded_of.at(merged.sources[source].plan_node);

            if (lazy.kind == LazySource::Kind::MergeTree)
            {
                /// Both in order, so they are followed at once.
                auto next_unneeded = unneeded.begin();
                size_t kept_index = 0;
                for (size_t index = 0; index < lazy.positions.size(); ++index)
                {
                    const size_t position = lazy.positions[index];
                    while (next_unneeded != unneeded.end() && *next_unneeded < position)
                        ++next_unneeded;

                    if (next_unneeded != unneeded.end() && *next_unneeded == position)
                    {
                        lazy.positions[kept_index] = position;
                        lazy.inputs[kept_index] = lazy.inputs[index];
                        ++kept_index;
                        continue;
                    }

                    kept_by_read.insert(lazy.inputs[index]);
                }
                lazy.positions.resize(kept_index);
                lazy.inputs.resize(kept_index);
            }
            else
            {
                const auto & inputs = merged.sources[source].inputs;
                const auto erased = std::erase_if(lazy.inputs, [&](const ActionsDAG::Node * input)
                {
                    const size_t position = std::ranges::find(inputs, input) - inputs.begin();
                    if (std::ranges::binary_search(unneeded, position))
                        return false;
                    lazy.eager_names.insert(input->result_name);
                    kept_by_read.insert(input);
                    return true;
                });

                if (erased != 0 && !settleObjectStorageLazyRead(source, lazy, kept_by_read))
                    return false;
            }

            if (lazy.inputs.empty())
                it = lazy_by_source.erase(it);
            else
                ++it;
        }
        return true;
    }

    /// `sorting_node` is the full sorting right below `root`, the `LIMIT`, or null without ORDER BY.
    void apply(QueryPlan & query_plan, QueryPlan::Nodes & nodes, QueryPlan::Node & root, QueryPlan::Node * sorting_node)
    {
        /// An expression or a filter computing a carried value it does not output outputs a copy of it last. The steps
        /// above it still have its former header, which the copy comes after, as the other columns this rebuild adds.
        for (const auto & [node, indices] : carried_from)
            for (size_t index : indices)
                if (const auto & carrying = carried[index]; carrying.step_node)
                    appendOutputToStep(*node->step, *carrying.step_node, carrying.name);

        auto assembled = assemble(nodes);
        auto & main_plan = assembled.plan;

        /// Hand over exactly the sort keys, the crossing values and the row indexes; the sort then carries no
        /// more than that. What each column of the block holds is followed from here on, so that the
        /// expression above the `LIMIT` can bind by position.
        const auto & outputs = merged.getOutputs();
        std::vector<Slot> slots;
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
                slots.push_back({.value = outputs[position]});
            }
            for (const auto & carrying : carried)
            {
                projection_outputs.push_back(inputs.at(header.getPositionByName(carrying.name)));
                slots.push_back({.value = carrying.value});
            }
            for (size_t source : sources_with_row_index)
            {
                projection_outputs.push_back(inputs[header.getPositionByName(makeRowIndexName(source))]);
                slots.push_back({.kind = Slot::Kind::RowIndex, .source = source});
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
            for (size_t source : sources_with_mask)
            {
                const auto * index = masks_dag.getInputs()[header.getPositionByName(makeRowIndexName(source))];
                masks_dag.getOutputs().push_back(&masks_dag.addFunction(is_not_null, {index}, makeMaskName(source)));
                slots.push_back({.kind = Slot::Kind::Mask, .source = source});
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

            /// The step takes the row index away.
            const auto index_slot = std::ranges::find_if(
                slots, [&](const Slot & slot) { return slot.kind == Slot::Kind::RowIndex && slot.source == source; });
            if (index_slot == slots.end())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "The row index of source {} does not reach the lazy read", source);
            slots.erase(index_slot);

            /// The lazy read returns its columns in the order it reads them from storage rather than in the order of the
            /// header of the source, so each is matched with an input it was chosen for by name. Inputs of the same name are
            /// the same column of storage, so which of them takes which column does not matter.
            std::vector<bool> is_bound(lazy.inputs.size(), false);
            for (const auto & column : *lazy_plan.getCurrentHeader())
            {
                size_t input = 0;
                while (input < lazy.inputs.size() && (is_bound[input] || lazy.inputs[input]->result_name != column.name))
                    ++input;
                if (input == lazy.inputs.size())
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "The lazy read of source {} returns the column {}, which no input was chosen for", source, column.name);
                is_bound[input] = true;
                slots.push_back({.value = lazy.inputs[input]});
            }

            auto join_lazy_columns = std::make_unique<JoinLazyColumnsStep>(
                main_plan.getCurrentHeader(), lazy_plan.getCurrentHeader(), lazy_materializing_rows, makeRowIndexName(source));

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
            std::unordered_map<size_t, size_t> mask_position_of_source;
            for (size_t position = 0; position < slots.size(); ++position)
            {
                const auto & slot = slots[position];
                if (slot.kind == Slot::Kind::Value)
                    positions.emplace(slot.value, position);
                else if (slot.kind == Slot::Kind::Mask)
                    mask_position_of_source.emplace(slot.source, position);
            }

            std::unordered_map<const MergedPlanDAG::PaddedSide *, size_t> mask_slots;
            for (const auto & [padded_side, source] : mask_sources)
                mask_slots.emplace(padded_side, mask_position_of_source.at(source));

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
        enum class Kind : uint8_t
        {
            MergeTree,
            ObjectStorage,
        };
        Kind kind = Kind::MergeTree;

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

            lazy.kind = LazySource::Kind::ObjectStorage;
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

        return true;
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

    bool isAddedColumn(const String & name) const { return added_names.contains(name); }

    /// A rebuilt node: its plan, and the positions of its former header it no longer has. Its header is the former one
    /// without those positions, in the same order, then what its step appended, then the columns this rebuild added.
    struct Assembled
    {
        QueryPlan plan;
        std::vector<size_t> dropped_positions;
        /// Whether this rebuild put a step of its own on top of the plan - the row index of a read, the copies of carried
        /// columns - which is merged into the step above when that can take it.
        bool has_added_step_on_top = false;
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

    /// What each node between `chain_top` and the sources, `top_down_nodes`, need not output, top-down, as `assemble`
    /// prunes them: what the step above does not need of it, less the columns values are carried from. The chain hands
    /// over the sort keys and the copies of the values crossing the `LIMIT`; its header is the outputs of the merged DAG.
    std::unordered_map<const QueryPlan::Node *, std::vector<size_t>> findUnneededColumns(const std::vector<QueryPlan::Node *> & top_down_nodes) const
    {
        std::vector<size_t> chain_top_unneeded;
        auto next_sort_key = sort_key_positions.begin();
        for (size_t position = 0; position < merged.getOutputs().size(); ++position)
        {
            if (next_sort_key != sort_key_positions.end() && *next_sort_key == position)
                ++next_sort_key;
            else
                chain_top_unneeded.push_back(position);
        }

        /// A node a value is carried from keeps the column it is copied from.
        std::unordered_map<const QueryPlan::Node *, std::vector<size_t>> unneeded_of{{chain_top, std::move(chain_top_unneeded)}};
        for (auto * node : top_down_nodes)
        {
            auto & node_unneeded = unneeded_of.at(node);
            if (const auto it = carried_from.find(node); it != carried_from.end())
                for (size_t index : it->second)
                    if (!carried[index].step_node)
                        std::erase(node_unneeded, carried[index].position);

            if (source_numbers.contains(node))
                continue;

            auto child_unneeded = node->step->getUnneededColumns(node_unneeded);
            for (size_t child = 0; child < node->children.size(); ++child)
                unneeded_of[node->children[child]] = std::move(child_unneeded.at(child));
        }
        return unneeded_of;
    }

    /// Builds the main branch: prunes from each step what the step above does not need of it, top-down, and
    /// assembles it bottom-up - splits the lazily read columns off the reads, adds the row indexes, which
    /// the joins pass through, and prunes a step once its children are built, with their real headers.
    Assembled assemble(QueryPlan::Nodes & nodes)
    {
        const auto top_down_nodes = collectNodes();
        const auto unneeded_of = findUnneededColumns(top_down_nodes);

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
        const auto it = carried_from.find(node);
        if (it == carried_from.end())
            return copies;

        for (size_t index : it->second)
        {
            const auto & carrying = carried[index];
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
        assembled.plan.addStep(std::move(step));
        assembled.has_added_step_on_top = true;
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
            const bool is_above_added_step = children.front().has_added_step_on_top;
            result.plan.addStep(std::move(node->step));
            if (is_above_added_step)
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
        if (!lazy && !sources_with_mask.contains(source))
        {
            if (!copies.empty())
            {
                addCopiesOfCarriedColumns(node, result);
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
        sources_with_row_index.insert(source);
        added_names.insert(index_name);

        /// Below a join that can leave the source unmatched the index is `Nullable`.
        const bool nullable_index = merged.getNearestPaddedSideOfSource(source) != nullptr;

        auto above_read = makeAboveReadDAG(*plan.getCurrentHeader(), index_positions, consumed_positions, index_name, nullable_index, copies);
        auto step = std::make_unique<ExpressionStep>(plan.getCurrentHeader(), std::move(above_read));
        step->setStepDescription("Row index");
        result.has_added_step_on_top = true;
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

    QueryPlan::Node * const chain_top;
    const std::vector<size_t> & sort_key_positions;
    std::unordered_map<const QueryPlan::Node *, size_t> source_numbers;
    std::map<size_t, LazySource> lazy_by_source;

    /// What a column of the block holds from the columns crossing the `LIMIT` on: a value of the merged DAG, the row index
    /// of a source, or the mask a source gives.
    struct Slot
    {
        enum class Kind : uint8_t
        {
            Value,
            RowIndex,
            Mask,
        };
        Kind kind = Kind::Value;
        const ActionsDAG::Node * value = nullptr;
        size_t source = 0;
    };

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
    /// The carried values, in the order of the merged DAG, which is the order their copies cross the `LIMIT` in, and by the
    /// node each is carried from, in the same order.
    std::vector<Carrying> carried;
    std::unordered_map<const QueryPlan::Node *, std::vector<size_t>> carried_from;

    /// The row index of every source that has one, either for a lazy read or for a mask.
    std::set<size_t> sources_with_row_index;
    /// The sources whose row index gives a mask, from `mask_sources`.
    std::set<size_t> sources_with_mask;
    NameSet added_names;
};

/// Whether a step can produce a block of totals: `WITH TOTALS`, or a remote read, whose query can have it. Such a block
/// goes through the joins up to the `LIMIT` with no rows to read lazily, so it would reach the expression above the
/// `LIMIT` without the columns read lazily; and a value computed below a join is that join's default in it, which
/// recomputing the value above the `LIMIT` would not give.
bool canProduceTotals(const IQueryPlanStep & step)
{
    return typeid_cast<const TotalsHavingStep *>(&step) || typeid_cast<const ReadFromRemote *>(&step)
        || typeid_cast<const ReadFromParallelRemoteReplicasStep *>(&step);
}

/// Whether a step in the subtree of `node` can produce a block of totals, see `canProduceTotals`. The plans a step owns
/// count as well - the plan of a sealed view, the plans of the tables of a `Merge` table - as their totals come out of
/// that step.
bool subtreeCanProduceTotals(const QueryPlan::Node & node)
{
    std::vector<const QueryPlan::Node *> stack{&node};
    while (!stack.empty())
    {
        const auto * current = stack.back();
        stack.pop_back();
        if (canProduceTotals(*current->step))
            return true;
        stack.insert(stack.end(), current->children.begin(), current->children.end());
        for (const auto * child_plan : current->step->getChildPlans(/*for_explain=*/ false))
            if (child_plan && child_plan->isInitialized())
                stack.push_back(child_plan->getRootNode());
    }
    return false;
}

/// A cheap look at the plan before the merged DAG is built: the steps its walk can go through hold a join, and a source
/// they reach can be read lazily. The walk can stop higher up, so this only rules plans out. The columns this pass adds
/// are found by name, so a name of this pass in a header of those steps or sources rules the plan out as well, and so
/// does a source that can produce a block of totals.
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
            /// A source the walk does not go into can hide anything below it.
            if (subtreeCanProduceTotals(*node))
                return false;

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
    const QueryPlanOptimizationSettings & settings)
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
    const auto max_limit = settings.max_limit_for_lazy_materialization;
    if (limit == 0 || (max_limit != 0 && limit > max_limit))
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

    /// A subtree reading one source is what `optimizeLazyMaterialization2` handles. A join is the only step with more than
    /// one child the merged DAG goes through, so more sources than one mean a join.
    if (merged.sources.size() < 2)
        return false;

    std::vector<bool> lazy_sources(merged.sources.size(), false);
    for (size_t source = 0; source < merged.sources.size(); ++source)
        lazy_sources[source] = canReadLazily(*merged.sources[source].plan_node, settings);

    if (!std::ranges::contains(lazy_sources, true))
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
        auto * merge_tree = typeid_cast<ReadFromMergeTree *>(merged.sources[source].plan_node->step.get());
        if (!lazy_sources[source] || !merge_tree)
            continue;

        if (const auto * padded_side = merged.getNearestPaddedSideOfSource(source))
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

    /// The masks call functions, which need the context of a read they are computed from.
    if (!mask_sources.empty() && !context)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Lazy materialization over joins has masks, but no context to compute them");

    JoinPlanRebuild rebuild(merged, frontier, lazy_sources, mask_sources, masked_values, context, chain_top, sort_key_positions);
    if (!rebuild.prepare())
        return false;

    rebuild.apply(query_plan, nodes, root, sorting_node);
    return true;
}

}
