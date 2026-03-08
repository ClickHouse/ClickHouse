#include <memory>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/JoinExpressionActions.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/LazilyReadFromMergeTree.h>
#include <Processors/QueryPlan/JoinLazyColumnsStep.h>
#include <Processors/Transforms/LazyMaterializingTransform.h>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB::QueryPlanOptimizations
{

using StepStack = std::vector<IQueryPlanStep *>;

static bool canUseLazyMaterializationForReadingStep(ReadFromMergeTree * reading)
{
    if (reading->isQueryWithFinal())
        return false;

    if (reading->isQueryWithSampling())
        return false;

    if (reading->isVectorColumnReplaced())
        return false;

    return true;
}

/// Returns two vectors of total size equal to the number of columns in the header.
/// The first vector (size of `inputs.size()`) contains positions of the inputs in the header.
/// The second vector contains other positions (sorted).
std::pair<std::vector<size_t>, std::vector<size_t>> mapInputsToHeaderPositions(const ActionsDAG::NodeRawConstPtrs & inputs, const Block & header)
{
    std::unordered_map<std::string, std::list<size_t>> name_to_position;
    for (size_t i = 0; i < header.columns(); ++i)
        name_to_position[header.getByPosition(i).name].push_back(i);

    std::vector<size_t> positions;
    positions.reserve(inputs.size());
    for (const auto * input : inputs)
    {
        auto & lst = name_to_position[input->result_name];
        if (lst.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown identifier: '{}'", input->result_name);

        positions.push_back(lst.front());
        lst.pop_front();
    }

    std::vector<size_t> non_mapped;
    for (size_t i = 0; i < header.columns(); ++i)
        for (auto idx : name_to_position[header.getByPosition(i).name])
            non_mapped.push_back(idx);

    return {std::move(positions), std::move(non_mapped)};
}

/// Step is always Filter or Expression
struct PlanStepWithRequiredDAGPositions
{
    IQueryPlanStep * step;
    /// Positions correspond to the outputs of the internal DAG.
    /// For the FilterStep, it is before the removal of filter columns.
    std::vector<bool> required_positions;
};

/// Returns a boolean mask which indicate if the header column is required.
/// The required_output_positions is the same mask for the output header.
/// There may be less DAG outputs than required_output_positions.size().
std::vector<bool> getRequiredHeaderPositions(const ActionsDAG & dag, const Block & header, std::vector<bool> required_output_positions)
{
    std::unordered_set<const ActionsDAG::Node *> required_nodes;
    std::stack<const ActionsDAG::Node *> stack;

    for (size_t i = 0; i < dag.getOutputs().size(); ++i)
    {
        if (required_output_positions[i])
            stack.push(dag.getOutputs()[i]);
    }

    std::vector<bool> required_input_positions(header.columns(), false);

    while (!stack.empty())
    {
        const auto * current_node = stack.top();
        stack.pop();

        bool inserted = required_nodes.insert(current_node).second;
        if (!inserted)
            continue;

        for (const auto * child : current_node->children)
            stack.push(child);
    }

    const auto & inputs = dag.getInputs();
    const auto [header_positions, non_mapped] = mapInputsToHeaderPositions(inputs, header);
    for (size_t i = 0; i < inputs.size(); ++i)
        if (required_nodes.contains(inputs[i]))
            required_input_positions[header_positions[i]] = true;

    /// Used columns which are not DAG outputs should be forwarded to the input header.
    size_t num_outputs = dag.getOutputs().size();
    for (size_t i = 0; num_outputs + i < required_output_positions.size(); ++i)
        if (required_output_positions[num_outputs + i])
            required_input_positions[non_mapped[i]] = true;

    return required_input_positions;
}

/// Add filter column to required_output_positions.
void updateRequiredColumnsForFilterDAG(std::vector<bool> & required_output_positions, const FilterStep & filter_step)
{
    const auto & expression = filter_step.getExpression();
    const auto & name = filter_step.getFilterColumnName();
    const auto & outputs = expression.getOutputs();

    size_t i = 0;
    for (; i < outputs.size(); ++i)
    {
        if (outputs[i]->result_name == name)
            break;
    }

    if (filter_step.removesFilterColumn())
    {
        required_output_positions.push_back(false);
        for (size_t j = required_output_positions.size() - 1; j > i; --j)
            required_output_positions[j] = required_output_positions[j - 1];
    }

    required_output_positions[i] = true;
}

struct SplitExpressionStepResult
{
    ActionsDAG main_expression_step;
    ActionsDAG lazy_expression_step;

    /// Those are available input positions for the next step (main branch).
    std::vector<bool> available_input_positions;
};

/// Split if ActionsDAG can produce unused pair of input/output which only changes the order.
/// Remove them from the DAG.
void removeDanglingNodes(ActionsDAG & dag)
{
    std::unordered_set<const ActionsDAG::Node *> used_nodes;
    for (const auto & node : dag.getNodes())
        for (const auto * child : node.children)
            used_nodes.insert(child);

    std::unordered_set<const ActionsDAG::Node *> inputs;
    for (const auto & input : dag.getInputs())
        inputs.insert(input);

    auto & outputs = dag.getOutputs();
    size_t next_pos = 0;
    for (size_t i = 0; i < outputs.size(); ++i)
    {
        bool is_dangling = inputs.contains(outputs[i]) && !used_nodes.contains(outputs[i]);
        if (!is_dangling)
            outputs[next_pos++] = outputs[i];
    }
    outputs.resize(next_pos);
    dag.removeUnusedActions();
}

/// This function transitively adds ActionsDAG::Node into the set, if all the arguments are already in set (or constants).
/// It's useful because the main branch of lazy materialization can return more columns than actually required.
/// As an example, for the query `select a from table prewhere b > 0 order by c limit 1`, only columns `c` is required for ORDER BY,
/// but the column `a` is returned as well (it's need for PREWHERE).
void addRequiredInputDependenciesIntoNodesSet(const ActionsDAG & dag, std::unordered_set<const ActionsDAG::Node *> & nodes)
{
    std::unordered_set<const ActionsDAG::Node *> visited;
    struct Frame
    {
        const ActionsDAG::Node * node;
        size_t next_child = 0;
    };
    std::stack<Frame> stack;

    for (const auto & node : dag.getNodes())
    {
        if (visited.contains(&node))
            continue;

        visited.insert(&node);
        stack.push({&node});
        while (!stack.empty())
        {
            auto & frame = stack.top();

            if (frame.next_child < frame.node->children.size())
            {
                const auto * child = frame.node->children[frame.next_child++];
                if (visited.contains(child))
                    continue;
                visited.insert(child);
                stack.push({child});
                continue;
            }

            bool all_children_are_allowed = true;
            for (const auto * child : frame.node->children)
            {
                if (nodes.contains(child))
                    continue;

                while (child->type == ActionsDAG::ActionType::ALIAS)
                    child = child->children.front();

                if (child->column)
                    continue;

                all_children_are_allowed = false;
                break;
            }
            if (all_children_are_allowed && frame.node->type != ActionsDAG::ActionType::INPUT && !frame.node->column)
                nodes.insert(frame.node);

            stack.pop();
        }
    }
}

/// requred_outputs are outputs of ActionsDAG, however required_inputs are inputs corresponding to the step input header.
SplitExpressionStepResult splitExpressionStep(const ExpressionStep & expression_step, const std::vector<bool> & required_outputs, const std::vector<bool> & required_inputs)
{
    const auto & expression = expression_step.getExpression();
    const auto & inputs = expression.getInputs();
    const auto & outputs = expression.getOutputs();

    const auto & header = *expression_step.getInputHeaders().front();
    chassert(header.columns() == required_inputs.size());
    chassert(outputs.size() == required_outputs.size());

    const auto [header_positions, non_mapped] = mapInputsToHeaderPositions(inputs, header);

    std::unordered_set<const ActionsDAG::Node *> split_nodes;
    for (size_t i = 0; i < inputs.size(); ++i)
    {
        if (required_inputs[header_positions[i]])
            split_nodes.insert(inputs[i]);
    }

    for (size_t i = 0; i < outputs.size(); ++i)
    {
        if (required_outputs[i])
            split_nodes.insert(outputs[i]);
    }
    addRequiredInputDependenciesIntoNodesSet(expression, split_nodes);

    // std::cerr << "split nodes: " << split_nodes.size() << std::endl;
    // for (const auto * node : split_nodes)
    //     std::cerr << "  " << node->result_name << std::endl;

    auto split_result = expression.split(split_nodes, true, true);

    std::vector<bool> new_required_inputs(expression_step.getOutputHeader()->columns(), false);
    for (size_t ps = 0; ps < outputs.size(); ++ps)
        if (split_nodes.contains(outputs[ps]))
            new_required_inputs[ps] = true;

    /// Used columns which are not DAG outputs should be forwarded to the input header.
    size_t num_outputs = expression.getOutputs().size();
    for (size_t i = 0; i < non_mapped.size(); ++i)
        if (required_inputs[non_mapped[i]])
            new_required_inputs[num_outputs + i] = true;

    return { std::move(split_result.first), std::move(split_result.second), std::move(new_required_inputs) };
}

struct SplitFilterResult
{
    FilterDAGInfo main_filter_step;
    ActionsDAG lazy_expression_step;

    /// Those are available input positions for the next step (main branch).
    std::vector<bool> available_input_positions;
};

SplitFilterResult splitFilterStep(const FilterStep & filter_step, const std::vector<bool> & required_outputs, const std::vector<bool> & required_inputs)
{
    const auto & expression = filter_step.getExpression();
    const auto & name = filter_step.getFilterColumnName();
    const auto & inputs = expression.getInputs();
    const auto & outputs = expression.getOutputs();

    const auto & header = *filter_step.getInputHeaders().front();
    if (header.columns() != required_inputs.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Header columns count does not match required inputs count");
    if (outputs.size() != required_outputs.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Outputs size does not match required outputs size");

    const auto [header_positions, non_mapped] = mapInputsToHeaderPositions(inputs, header);

    std::unordered_set<const ActionsDAG::Node *> split_nodes;
    for (size_t i = 0; i < inputs.size(); ++i)
        if (required_inputs[header_positions[i]])
            split_nodes.insert(inputs[i]);

    for (size_t i = 0; i < outputs.size(); ++i)
    {
        if (required_outputs[i])
            split_nodes.insert(outputs[i]);
    }
    addRequiredInputDependenciesIntoNodesSet(expression, split_nodes);

    auto split_result = expression.split(split_nodes, true, true);

    std::vector<bool> new_required_inputs(filter_step.getOutputHeader()->columns() + (filter_step.removesFilterColumn() ? 1 : 0), false);
    for (size_t ps = 0; ps < outputs.size(); ++ps)
        if (split_nodes.contains(outputs[ps]))
            new_required_inputs[ps] = true;

    /// Used columns which are not DAG outputs should be forwarded to the input header.
    size_t num_outputs = expression.getOutputs().size();
    for (size_t i = 0; i < non_mapped.size(); ++i)
        if (required_inputs[non_mapped[i]])
            new_required_inputs[num_outputs + i] = true;

    if (filter_step.removesFilterColumn())
    {
        const auto & node = expression.findInOutputs(name);
        for (size_t i = 0; i < outputs.size(); ++i)
        {
            if (outputs[i] == &node)
            {
                new_required_inputs.erase(new_required_inputs.begin() + i);
                break;
            }
        }
    }

    FilterDAGInfo filter_dag_info;
    filter_dag_info.actions = std::move(split_result.first);
    filter_dag_info.column_name = name;
    filter_dag_info.do_remove_column = filter_step.removesFilterColumn();

    return { std::move(filter_dag_info), std::move(split_result.second), std::move(new_required_inputs) };
}

std::unique_ptr<LazilyReadFromMergeTree> removeUnusedColumnsFromReadingStep(ReadFromMergeTree & reading_step, const std::vector<bool> & required_output_positions)
{
    const auto & cols = reading_step.getOutputHeader()->getColumnsWithTypeAndName();
    chassert(cols.size() == required_output_positions.size());

    NameSet required_names;
    for (size_t i = 0; i < cols.size(); ++i)
        if (required_output_positions[i])
            required_names.insert(cols[i].name);

    return reading_step.keepOnlyRequiredColumnsAndCreateLazyReadStep(required_names);
}

ActionsDAG calculateGlobalOffset(ReadFromMergeTree & reading_step)
{
    bool added_part_starting_offset;
    bool added_part_offset;
    reading_step.addStartingPartOffsetAndPartOffset(added_part_starting_offset, added_part_offset);
    ActionsDAG dag;
    DataTypePtr uint64_type = std::make_shared<DataTypeUInt64>();
    dag.addInput("_part_starting_offset", uint64_type);
    dag.addInput("_part_offset", uint64_type);

    auto plus = FunctionFactory::instance().get("plus", nullptr);
    const auto * global_offset_node = &dag.addFunction(plus, dag.getInputs(), {});
    global_offset_node = &dag.addAlias(*global_offset_node, "__global_row_index");

    dag.getOutputs().push_back(global_offset_node);

    /// Remove virtual columns if they were not initially needed.
    if (!added_part_starting_offset)
        dag.getOutputs().push_back(dag.getInputs()[0]);
    if (!added_part_offset)
        dag.getOutputs().push_back(dag.getInputs()[1]);

    return dag;
}

static ReadFromMergeTree * findReadingStep(QueryPlan::Node & node, StepStack & backward_path)
{
    IQueryPlanStep * step = node.step.get();
    backward_path.push_back(step);

    if (auto * read_from_merge_tree = typeid_cast<ReadFromMergeTree *>(step))
        return read_from_merge_tree;

    if (node.children.size() != 1)
        return nullptr;

    if (typeid_cast<ExpressionStep *>(step) || typeid_cast<FilterStep *>(step))
        return findReadingStep(*node.children.front(), backward_path);

    return nullptr;
}

bool optimizeLazyMaterialization2(QueryPlan::Node & root, QueryPlan & query_plan, QueryPlan::Nodes & nodes, const QueryPlanOptimizationSettings & settings, size_t max_limit_for_lazy_materialization)
{
    if (root.children.size() != 1)
        return false;

    auto * limit_step = typeid_cast<LimitStep *>(root.step.get());
    if (!limit_step)
        return false;

    /// it's not clear how many values will be read for LIMIT WITH TIES, so disable it
    if (limit_step->withTies())
        return false;

    auto * sorting_step = typeid_cast<SortingStep *>(root.children.front()->step.get());
    if (!sorting_step)
        return false;

    if (sorting_step->getType() != SortingStep::Type::Full && sorting_step->getType() != SortingStep::Type::FinishSorting)
        return false;

    bool reading_in_order = sorting_step->getType() == SortingStep::Type::FinishSorting;

    const auto limit = limit_step->getLimit();
    if (limit == 0 || (max_limit_for_lazy_materialization != 0 && limit > max_limit_for_lazy_materialization))
        return false;

    StepStack steps_to_update;

    auto * sorting_node = root.children.front();
    auto * reading_step = findReadingStep(*sorting_node->children.front(), steps_to_update);
    if (!reading_step)
        return false;

    if (!canUseLazyMaterializationForReadingStep(reading_step))
        return false;

    const auto & sorting_header = *sorting_step->getOutputHeader();
    /// At this moment, required_columns are corresponding to output header columns of every step.
    std::vector<bool> required_columns(sorting_header.columns(), false);

    for (const auto & descr : sorting_step->getSortDescription())
        required_columns[sorting_header.getPositionByName(descr.column_name)] = true;

    bool has_filter = false;

    std::vector<PlanStepWithRequiredDAGPositions> steps_to_split;

    auto * node = sorting_node->children.front();
    while (!node->children.empty())
    {
        IQueryPlanStep * step = node->step.get();

        PlanStepWithRequiredDAGPositions step_to_split;
        step_to_split.step = step;

        // std::cerr << "::: req columns (" << required_columns.size() << ") [";
        // for (auto && required_column : required_columns)
        //     std::cerr << required_column << " ";
        // std::cerr << "]\n";

        if (const auto * expr_step = typeid_cast<ExpressionStep *>(step))
        {
            const auto & expr = expr_step->getExpression();
            /// The number of DAG outputs can be less then the number of columns in the header.
            step_to_split.required_positions.insert(step_to_split.required_positions.end(), required_columns.begin(), required_columns.begin() + expr.getOutputs().size());
            required_columns = getRequiredHeaderPositions(expr, *expr_step->getInputHeaders().front() , std::move(required_columns));
        }
        else if (const auto * filter_step = typeid_cast<FilterStep *>(step))
        {
            updateRequiredColumnsForFilterDAG(required_columns, *filter_step);
            const auto & expr = filter_step->getExpression();
            step_to_split.required_positions.insert(step_to_split.required_positions.end(), required_columns.begin(), required_columns.begin() + expr.getOutputs().size());
            required_columns = getRequiredHeaderPositions(expr, *filter_step->getInputHeaders().front(), std::move(required_columns));
            has_filter = true;
        }
        else
            return false;

        steps_to_split.push_back(std::move(step_to_split));
        node = node->children.front();
    }

    auto * read_from_merge_tree = typeid_cast<ReadFromMergeTree *>(node->step.get());
    if (!read_from_merge_tree)
        return false;

    if (read_from_merge_tree->getPrewhereInfo() || read_from_merge_tree->getRowLevelFilter())
        has_filter = true;

    /// Disable the case with read-in-order and no filter.
    /// It's not likely we can optimize it more.
    if (reading_in_order && !has_filter)
        return false;

    std::unique_ptr<LazilyReadFromMergeTree> lazy_reading;
    {
        auto initial_header = read_from_merge_tree->getOutputHeader();
        const auto & cols = initial_header->getColumnsWithTypeAndName();
        chassert(cols.size() == required_columns.size());

        NameSet required_names;
        for (size_t i = 0; i < cols.size(); ++i)
            if (required_columns[i])
                required_names.insert(cols[i].name);

        lazy_reading = read_from_merge_tree->keepOnlyRequiredColumnsAndCreateLazyReadStep(required_names);
        if (!lazy_reading)
            return false;

        const auto & header = *read_from_merge_tree->getOutputHeader();
        /// At this moment, required_columns are corresponding to available columns in the input header of every step.
        /// This is needed because read_from_merge_tree can return more columns than required.
        required_columns.assign(cols.size(), true);
        for (size_t i = 0; i < cols.size(); ++i)
            required_columns[i] = header.has(cols[i].name);

        // std::cerr << ".. Main header " << read_from_merge_tree->getOutputHeader()->dumpNames() << std::endl;
        // std::cerr << ".. Lazy header " << lazy_reading->getOutputHeader()->dumpNames() << std::endl;
    }

    std::list<std::variant<ActionsDAG, FilterDAGInfo>> main_steps;
    std::list<ActionsDAG> lazy_steps;

    for (const auto & step_to_split : steps_to_split | std::views::reverse)
    {
        // std::cerr << " req columns (" << required_columns.size() << ") [";
        // for (auto && required_column : required_columns)
        //     std::cerr << required_column << " ";
        // std::cerr << "]\n";

        if (const auto * expr_step = typeid_cast<ExpressionStep *>(step_to_split.step))
        {
            // std::cerr << "split_s: " << expr_step->getExpression().dumpDAG() << std::endl;
            auto split_result = splitExpressionStep(*expr_step, step_to_split.required_positions, required_columns);
            // std::cerr << "split_result l: " << split_result.main_expression_step.dumpDAG() << std::endl;
            // std::cerr << "split_result r: " << split_result.lazy_expression_step.dumpDAG() << std::endl;
            main_steps.push_back(std::move(split_result.main_expression_step));
            lazy_steps.push_back(std::move(split_result.lazy_expression_step));
            required_columns = std::move(split_result.available_input_positions);
        }
        else if (const auto * filter_step = typeid_cast<FilterStep *>(step_to_split.step))
        {
            // std::cerr << "fsplit_s: " << filter_step->getExpression().dumpDAG() << std::endl;
            auto split_result = splitFilterStep(*filter_step, step_to_split.required_positions, required_columns);
            // std::cerr << "fsplit_result l: " << split_result.main_filter_step.actions.dumpDAG() << std::endl;
            // std::cerr << "fsplit_result r: " << split_result.lazy_expression_step.dumpDAG() << std::endl;
            main_steps.push_back(std::move(split_result.main_filter_step));
            lazy_steps.push_back(std::move(split_result.lazy_expression_step));
            required_columns = std::move(split_result.available_input_positions);
        }
        else
            return false;
    }

    QueryPlan main_plan;
    QueryPlan lazy_plan;

    auto main_global_offset_dag = calculateGlobalOffset(*read_from_merge_tree);

    main_plan.addStep(std::move(node->step));
    auto main_global_offset_step = std::make_unique<ExpressionStep>(main_plan.getCurrentHeader(), std::move(main_global_offset_dag));
    main_plan.addStep(std::move(main_global_offset_step));

    for (auto & step : main_steps)
    {
        if (std::holds_alternative<ActionsDAG>(step))
        {
            auto dag = std::move(std::get<ActionsDAG>(step));
            main_plan.addStep(std::make_unique<ExpressionStep>(main_plan.getCurrentHeader(), std::move(dag)));
        }
        else
        {
            auto filter_dag_info = std::move(std::get<FilterDAGInfo>(step));
            main_plan.addStep(std::make_unique<FilterStep>(
                main_plan.getCurrentHeader(),
                std::move(filter_dag_info.actions), filter_dag_info.column_name, filter_dag_info.do_remove_column));
        }
    }

    auto new_sorting_step = std::move(root.children.front()->step); // = std::make_unique<SortingStep>(main_plan.getCurrentHeader(), sorting_step->getSortDescription(), sorting_step->getLimit(), sorting_step->getSettings());
    new_sorting_step->updateInputHeader(main_plan.getCurrentHeader());
    main_plan.addStep(std::move(new_sorting_step));

    limit_step->updateInputHeader(main_plan.getCurrentHeader());
    main_plan.addStep(std::move(root.step));

    auto lazy_materializing_rows = std::make_shared<LazyMaterializingRows>(read_from_merge_tree->getParts());
    lazy_reading->setLazyMaterializingRows(lazy_materializing_rows);
    lazy_plan.addStep(std::move(lazy_reading));

    const auto & lhs_plan_header = main_plan.getCurrentHeader();
    const auto & rhs_plan_header = lazy_plan.getCurrentHeader();

    auto join_lazy_columns = std::make_unique<JoinLazyColumnsStep>(lhs_plan_header, rhs_plan_header, lazy_materializing_rows);

    QueryPlan result_plan;

    std::vector<QueryPlanPtr> plans;
    plans.emplace_back(std::make_unique<QueryPlan>(std::move(main_plan)));
    plans.emplace_back(std::make_unique<QueryPlan>(std::move(lazy_plan)));

    result_plan.unitePlans(std::move(join_lazy_columns), {std::move(plans)});

    // {
    //     WriteBufferFromOwnString out;
    //     result_plan.explainPlan(out, {.header=true, .actions=true});
    //     std::cerr << out.str() << std::endl;
    // }

    convertLogicalJoinToPhysical(*result_plan.getRootNode(), nodes, settings);

    // {
    //     WriteBufferFromOwnString out;
    //     result_plan.explainPlan(out, {.header=true, .actions=true});
    //     std::cerr << out.str() << std::endl;
    // }

    // {
    //     WriteBufferFromOwnString out;
    //     result_plan.explainPlan(out, {.header=true, .actions=true});
    //     std::cerr << out.str() << std::endl;
    // }

    while (!lazy_steps.empty())
    {
        auto dag = std::move(lazy_steps.front());
        lazy_steps.pop_front();
        /// Remove dangling nodes from the DAG. Some of them may not exist anymore (e.g. filter column)
        if (!lazy_steps.empty())
            removeDanglingNodes(dag);
        result_plan.addStep(std::make_unique<ExpressionStep>(result_plan.getCurrentHeader(), std::move(dag)));
    }

    query_plan.replaceNodeWithPlan(&root, std::move(result_plan));

    return true;
}

}
