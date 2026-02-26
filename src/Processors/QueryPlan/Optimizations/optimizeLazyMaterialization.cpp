#include <memory>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/LazilyReadStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Storages/MergeTree/MergeTreeLazilyReader.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/JoinExpressionActions.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/LazilyReadFromMergeTree.h>
#include <Processors/QueryPlan/JoinLazyColumnsStep.h>
#include <Processors/Transforms/LazilyMaterializingTransform.h>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB::QueryPlanOptimizations
{

using StepStack = std::vector<IQueryPlanStep *>;

static bool canUseLazyMaterializationForReadingStep(ReadFromMergeTree * reading)
{
    if (reading->getLazilyReadInfo())
       return false;

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
    std::vector<bool> required_input_positions;
    ActionsDAG main_expression_step;
    ActionsDAG lazy_expression_step;
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

SplitExpressionStepResult splitExpressionStep(const ExpressionStep & expression_step, std::vector<bool> required_output_positions)
{
    const auto & expression = expression_step.getExpression();
    const auto & outputs = expression.getOutputs();

    std::unordered_set<const ActionsDAG::Node *> split_nodes;
    for (size_t i = 0; i < outputs.size(); ++i)
    {
        if (required_output_positions[i])
            split_nodes.insert(outputs[i]);
    }
    auto split_result = expression.split(split_nodes, true, true);

    auto required_input_positions = getRequiredHeaderPositions(expression, *expression_step.getInputHeaders().front(), std::move(required_output_positions));
    return { std::move(required_input_positions), std::move(split_result.first), std::move(split_result.second) };
}

struct SplitFilterResult
{
    std::vector<bool> required_input_positions;
    FilterDAGInfo main_filter_step;
    ActionsDAG lazy_expression_step;
};

SplitFilterResult splitFilterStep(const FilterStep & filter_step, std::vector<bool> required_output_positions)
{
    const auto & expression = filter_step.getExpression();
    const auto & name = filter_step.getFilterColumnName();

    const auto & outputs = expression.getOutputs();
    std::unordered_set<const ActionsDAG::Node *> split_nodes;

    updateRequiredColumnsForFilterDAG(required_output_positions, filter_step);

    for (size_t i = 0; i < outputs.size(); ++i)
    {
        if (required_output_positions[i])
            split_nodes.insert(outputs[i]);
    }

    auto split_result = expression.split(split_nodes, true, true);

    auto required_input_positions = getRequiredHeaderPositions(expression, *filter_step.getInputHeaders().front(), std::move(required_output_positions));

    FilterDAGInfo filter_dag_info;
    filter_dag_info.actions = std::move(split_result.first);
    filter_dag_info.column_name = name;
    filter_dag_info.do_remove_column = filter_step.removesFilterColumn();

    return { std::move(required_input_positions), std::move(filter_dag_info), std::move(split_result.second) };
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

static void removeUsedColumnNames(
    const ActionsDAG & actions,
    NameSet & lazily_read_column_name_set,
    AliasToName & alias_index,
    String filter_name = {})
{
    const auto & actions_outputs = actions.getOutputs();

    for (const auto * output_node : actions_outputs)
    {
        const auto * node = output_node;
        while (node && node->type == ActionsDAG::ActionType::ALIAS)
        {
            /// alias has only one child
            chassert(node->children.size() == 1);
            node = node->children.front();
        }

        if (!node)
            continue;

        if (node->type == ActionsDAG::ActionType::FUNCTION || node->type == ActionsDAG::ActionType::ARRAY_JOIN
            || (!filter_name.empty() && filter_name == output_node->result_name))
        {
            using ActionsNode = ActionsDAG::Node;

            std::unordered_set<const ActionsNode *> visited_nodes;
            std::stack<const ActionsNode *> stack;

            stack.push(node);
            while (!stack.empty())
            {
                const auto * current_node = stack.top();
                stack.pop();

                if (current_node->type == ActionsDAG::ActionType::INPUT)
                {
                    const auto it = alias_index.find(current_node->result_name);
                    if (it != alias_index.end())
                        lazily_read_column_name_set.erase(it->second);
                }

                for (const auto * child : current_node->children)
                {
                    if (!visited_nodes.contains(child))
                    {
                        stack.push(child);
                        visited_nodes.insert(child);
                    }
                }
            }
        }
    }

    /// Update alias name index.
    for (const auto * output_node : actions_outputs)
    {
        const auto * node = output_node;
        while (node && node->type == ActionsDAG::ActionType::ALIAS)
        {
            /// alias has only one child
            chassert(node->children.size() == 1);
            node = node->children.front();
        }
        if (node && node != output_node && node->type == ActionsDAG::ActionType::INPUT)
        {
            const auto it = alias_index.find(node->result_name);
            if (it != alias_index.end())
            {
                const auto real_column_name = it->second;
                alias_index.emplace(output_node->result_name, real_column_name);
                alias_index.erase(node->result_name);
            }
        }
    }
}

static void collectLazilyReadColumnNames(
    const StepStack & steps,
    ColumnsWithTypeAndName & lazily_read_columns,
    AliasToName & alias_index)
{
    auto * read_from_merge_tree = typeid_cast<ReadFromMergeTree *>(steps.back());
    const Names & all_column_names = read_from_merge_tree->getAllColumnNames();
    auto storage_snapshot = read_from_merge_tree->getStorageSnapshot();
    NameSet lazily_read_column_name_set;

    const auto options = GetColumnsOptions(GetColumnsOptions::AllPhysical)
        .withSubcolumns(storage_snapshot->storage.supportsSubcolumns());

    for (const auto & column_name : all_column_names)
    {
        if (storage_snapshot->tryGetColumn(options, column_name))
            lazily_read_column_name_set.insert(column_name);
    }

    for (const auto & column_name : lazily_read_column_name_set)
        alias_index.emplace(column_name, column_name);

    if (const auto & row_level_filter = read_from_merge_tree->getRowLevelFilter())
        removeUsedColumnNames(row_level_filter->actions, lazily_read_column_name_set, alias_index, row_level_filter->column_name);

    if (const auto & prewhere_info = read_from_merge_tree->getPrewhereInfo())
        removeUsedColumnNames(prewhere_info->prewhere_actions, lazily_read_column_name_set, alias_index, prewhere_info->prewhere_column_name);

    for (auto step_it = steps.rbegin(); step_it != steps.rend(); ++step_it)
    {
        auto * step = *step_it;

        if (lazily_read_column_name_set.empty())
            return;

        if (auto * expression_step = typeid_cast<ExpressionStep *>(step))
        {
            removeUsedColumnNames(expression_step->getExpression(), lazily_read_column_name_set, alias_index);
            continue;
        }

        if (auto * filter_step = typeid_cast<FilterStep *>(step))
        {
            removeUsedColumnNames(filter_step->getExpression(), lazily_read_column_name_set, alias_index, filter_step->getFilterColumnName());
            continue;
        }

        if (auto * sorting_step = typeid_cast<SortingStep *>(step))
        {
            const auto & sort_description = sorting_step->getSortDescription();
            for (const auto & sort_column_description : sort_description)
            {
                const auto it = alias_index.find(sort_column_description.column_name);
                if (it == alias_index.end())
                    continue;
                lazily_read_column_name_set.erase(it->second);
            }
            continue;
        }
    }

    lazily_read_columns.reserve(lazily_read_column_name_set.size());

    for (const auto & column_name : lazily_read_column_name_set)
    {
        auto name_and_type = storage_snapshot->tryGetColumn(options, column_name);
        lazily_read_columns.emplace_back(
            name_and_type->type->createColumn(),
            name_and_type->type,
            name_and_type->name);
    }
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

static void updateStepsDataStreams(StepStack & steps_to_update)
{
    /// update output data stream for found transforms
    if (!steps_to_update.empty())
    {
        const auto * input_header = &steps_to_update.back()->getOutputHeader();
        chassert(dynamic_cast<ReadFromMergeTree *>(steps_to_update.back()));
        steps_to_update.pop_back();

        while (!steps_to_update.empty())
        {
            auto * transforming_step = dynamic_cast<ITransformingStep *>(steps_to_update.back());
            chassert(transforming_step);

            transforming_step->updateInputHeader(*input_header);
            input_header = &steps_to_update.back()->getOutputHeader();
            steps_to_update.pop_back();
        }
    }
}

bool optimizeLazyMaterialization(QueryPlan::Node & root, Stack & stack, QueryPlan::Nodes & nodes, size_t max_limit_for_lazy_materialization)
{
    const auto & frame = stack.back();

    if (frame.node->children.size() != 1)
        return false;

    auto * limit_step = typeid_cast<LimitStep *>(frame.node->step.get());
    if (!limit_step)
        return false;

    /// it's not clear how many values will be read for LIMIT WITH TIES, so disable it
    if (limit_step->withTies())
        return false;

    auto * sorting_step = typeid_cast<SortingStep *>(frame.node->children.front()->step.get());
    if (!sorting_step)
        return false;

    if (sorting_step->getType() != SortingStep::Type::Full && sorting_step->getType() != SortingStep::Type::FinishSorting)
        return false;

    const auto limit = limit_step->getLimit();
    if (limit == 0 || (max_limit_for_lazy_materialization != 0 && limit > max_limit_for_lazy_materialization))
        return false;

    StepStack steps_to_update;
    steps_to_update.push_back(limit_step);
    steps_to_update.push_back(sorting_step);

    auto * sorting_node = frame.node->children.front();
    auto * reading_step = findReadingStep(*sorting_node->children.front(), steps_to_update);
    if (!reading_step)
        return false;

    if (!canUseLazyMaterializationForReadingStep(reading_step))
        return false;

    LazilyReadInfoPtr lazily_read_info = std::make_shared<LazilyReadInfo>();
    AliasToName alias_index;
    collectLazilyReadColumnNames(steps_to_update, lazily_read_info->lazily_read_columns, alias_index);

    if (lazily_read_info->lazily_read_columns.empty())
        return false;

    /// avoid applying this optimization on impractical queries for sake of implementation simplicity
    /// i.e. when no column used in query until projection, for example, select * from t order by rand() limit 10
    if (reading_step->getAllColumnNames().size() == lazily_read_info->lazily_read_columns.size())
        return false;

    auto storage_snapshot = reading_step->getStorageSnapshot();

    /// Snapshot data may be missed, for example, in EXPLAIN query.
    if (storage_snapshot->data)
    {
        auto mutations_snapshot = assert_cast<const MergeTreeData::SnapshotData &>(*storage_snapshot->data).mutations_snapshot;
        /// Applying patches in MergeTreeLazilyReader is not implemented.
        if (mutations_snapshot->hasPatchParts())
            return false;
    }

    lazily_read_info->data_part_infos = std::make_shared<DataPartInfoByIndex>();

    auto lazy_column_reader = std::make_unique<MergeTreeLazilyReader>(
        sorting_step->getOutputHeader(),
        reading_step->getMergeTreeData(),
        storage_snapshot,
        lazily_read_info,
        reading_step->getContext(),
        alias_index);

    reading_step->updateLazilyReadInfo(lazily_read_info);

    QueryPlan::Node * limit_node = frame.node;
    auto lazily_read_step
        = std::make_unique<LazilyReadStep>(sorting_step->getOutputHeader(), lazily_read_info, std::move(lazy_column_reader));
    lazily_read_step->setStepDescription("Lazily Read");

    /// the root node can be a limit node when query is distributed
    /// and executed on remote node till WithMergeableStateAfterAggregationAndLimit stage
    /// see 03404_lazy_materialization_distributed.sql
    if (limit_node == &root)
    {
        chassert(stack.size() == 1);

        /// move out limit from root node and move lazy read in
        auto & new_limit_node = nodes.emplace_back();
        new_limit_node.step = std::move(limit_node->step);
        new_limit_node.children = limit_node->children;

        root.children.clear();
        root.children.push_back(&new_limit_node);
        root.step = std::move(lazily_read_step);
    }
    else
    {
        chassert(stack.size() > 1);

        auto & lazy_read_node = nodes.emplace_back();
        lazy_read_node.step = std::move(lazily_read_step);
        lazy_read_node.children.emplace_back(limit_node);

        QueryPlan::Node * limit_parent_node = (stack.rbegin() + 1)->node;
        chassert(limit_parent_node);
        for (auto & limit_parent_child : limit_parent_node->children)
        {
            if (limit_parent_child == limit_node)
            {
                limit_parent_child = &lazy_read_node;
                break;
            }
        }
    }

    updateStepsDataStreams(steps_to_update);

    return true;
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
    std::vector<bool> required_columns(sorting_header.columns(), false);

    for (const auto & descr : sorting_step->getSortDescription())
        required_columns[sorting_header.getPositionByName(descr.column_name)] = true;

    bool has_filter = false;

    auto * node = sorting_node->children.front();
    while (!node->children.empty())
    {
        IQueryPlanStep * step = node->step.get();

        if (const auto * expr_step = typeid_cast<ExpressionStep *>(step))
            required_columns = getRequiredHeaderPositions(expr_step->getExpression(), *expr_step->getInputHeaders().front() , std::move(required_columns));
        else if (const auto * filter_step = typeid_cast<FilterStep *>(step))
        {
            updateRequiredColumnsForFilterDAG(required_columns, *filter_step);
            required_columns = getRequiredHeaderPositions(filter_step->getExpression(), *filter_step->getInputHeaders().front(), std::move(required_columns));
            has_filter = true;
        }
        else
            return false;

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

    auto lazy_reading = removeUnusedColumnsFromReadingStep(*read_from_merge_tree, required_columns);
    if (!lazy_reading)
        return false;

    std::list<std::variant<ActionsDAG, FilterDAGInfo>> main_steps;
    std::list<ActionsDAG> lazy_steps;

    required_columns.assign(sorting_header.columns(), false);

    for (const auto & descr : sorting_step->getSortDescription())
        required_columns[sorting_header.getPositionByName(descr.column_name)] = true;

    node = sorting_node->children.front();
    while (!node->children.empty())
    {
        IQueryPlanStep * step = node->step.get();

        if (const auto * expr_step = typeid_cast<ExpressionStep *>(step))
        {
            // std::cerr << "split_s: " << expr_step->getExpression().dumpDAG() << std::endl;
            auto split_result = splitExpressionStep(*expr_step, std::move(required_columns));
            // std::cerr << "split_result l: " << split_result.main_expression_step.dumpDAG() << std::endl;
            // std::cerr << "split_result r: " << split_result.lazy_expression_step.dumpDAG() << std::endl;
            main_steps.push_front(std::move(split_result.main_expression_step));
            lazy_steps.push_front(std::move(split_result.lazy_expression_step));
            required_columns = std::move(split_result.required_input_positions);
        }
        else if (const auto * filter_step = typeid_cast<FilterStep *>(step))
        {
            // std::cerr << "fsplit_s: " << filter_step->getExpression().dumpDAG() << std::endl;
            auto split_result = splitFilterStep(*filter_step, std::move(required_columns));
            // std::cerr << "fsplit_result l: " << split_result.main_filter_step.actions.dumpDAG() << std::endl;
            // std::cerr << "fsplit_result r: " << split_result.lazy_expression_step.dumpDAG() << std::endl;
            main_steps.push_front(std::move(split_result.main_filter_step));
            lazy_steps.push_front(std::move(split_result.lazy_expression_step));
            required_columns = std::move(split_result.required_input_positions);
        }
        else
            return false;

        node = node->children.front();
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
