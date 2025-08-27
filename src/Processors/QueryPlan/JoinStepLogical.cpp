#include <Processors/QueryPlan/JoinStepLogical.h>

#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>

#include <algorithm>
#include <ranges>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <IO/Operators.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/JoinUtils.h>
#include <Interpreters/PasteJoin.h>
#include <Interpreters/TableJoin.h>
#include <Planner/PlannerJoins.h>
#include <Processors/Transforms/JoiningTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/StorageJoin.h>
#include <Common/JSONBuilder.h>
#include <Common/typeid_cast.h>

#include <Functions/FunctionsLogical.h>
#include <Functions/FunctionsComparison.h>
#include <Functions/isNotDistinctFrom.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/IsOperation.h>
#include <Functions/tuple.h>


namespace DB
{

namespace Setting
{
    extern const SettingsJoinAlgorithm join_algorithm;
    extern const SettingsBool join_any_take_last_row;
    extern const SettingsUInt64 default_max_bytes_in_join;
    extern const SettingsBool join_use_nulls;
}

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
    extern const int INVALID_JOIN_ON_EXPRESSION;
    extern const int NOT_FOUND_COLUMN_IN_BLOCK;
    extern const int INCORRECT_DATA;
}

std::string operatorToFunctionName(PredicateOperator op)
{
    switch (op)
    {
        case PredicateOperator::NullSafeEquals: return FunctionIsNotDistinctFrom::name;
        case PredicateOperator::Equals: return NameEquals::name;
        case PredicateOperator::Less: return NameLess::name;
        case PredicateOperator::LessOrEquals: return NameLessOrEquals::name;
        case PredicateOperator::Greater: return NameGreater::name;
        case PredicateOperator::GreaterOrEquals: return NameGreaterOrEquals::name;
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Illegal value for PredicateOperator: {}", static_cast<Int32>(op));
}

std::optional<ASOFJoinInequality> operatorToAsofInequality(PredicateOperator op)
{
    switch (op)
    {
        case PredicateOperator::Less: return ASOFJoinInequality::Less;
        case PredicateOperator::LessOrEquals: return ASOFJoinInequality::LessOrEquals;
        case PredicateOperator::Greater: return ASOFJoinInequality::Greater;
        case PredicateOperator::GreaterOrEquals: return ASOFJoinInequality::GreaterOrEquals;
        default: return {};
    }
}

void formatJoinCondition(const JoinCondition & join_condition, WriteBuffer & buf)
{
    auto quote_string = std::views::transform([](const auto & s) { return fmt::format("({})", s.getColumnName()); });
    auto format_predicate = std::views::transform([](const auto & p) { return fmt::format("{} {} {}", p.left_node.getColumnName(), toString(p.op), p.right_node.getColumnName()); });
    buf << "[";
    buf << fmt::format("Predicates: ({})", fmt::join(join_condition.predicates | format_predicate, ", "));
    if (!join_condition.left_filter_conditions.empty())
        buf << " " << fmt::format("Left filter: ({})", fmt::join(join_condition.left_filter_conditions | quote_string, ", "));
    if (!join_condition.right_filter_conditions.empty())
        buf << " " << fmt::format("Right filter: ({})", fmt::join(join_condition.right_filter_conditions | quote_string, ", "));
    if (!join_condition.residual_conditions.empty())
        buf << " " << fmt::format("Residual filter: ({})", fmt::join(join_condition.residual_conditions | quote_string, ", "));
    buf << "]";
}

String formatJoinCondition(const JoinCondition & join_condition)
{
    WriteBufferFromOwnString buf;
    formatJoinCondition(join_condition, buf);
    return buf.str();
}

JoinStepLogical::JoinStepLogical(
    SharedHeader left_header_,
    SharedHeader right_header_,
    JoinInfo join_info_,
    JoinExpressionActions join_expression_actions_,
    Names required_output_columns_,
    bool use_nulls_,
    JoinSettings join_settings_,
    SortingStep::Settings sorting_settings_)
    : expression_actions(std::move(join_expression_actions_))
    , join_info(std::move(join_info_))
    , required_output_columns(std::move(required_output_columns_))
    , use_nulls(use_nulls_)
    , join_settings(std::move(join_settings_))
    , sorting_settings(std::move(sorting_settings_))
{
    updateInputHeaders({left_header_, right_header_});
}

QueryPipelineBuilderPtr JoinStepLogical::updatePipeline(QueryPipelineBuilders /* pipelines */, const BuildQueryPipelineSettings & /* settings */)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Cannot execute JoinStepLogical, it should be converted physical step first");
}

void JoinStepLogical::describePipeline(FormatSettings & settings) const
{
    IQueryPlanStep::describePipeline(processors, settings);
}

std::vector<std::pair<String, String>> JoinStepLogical::describeJoinActions() const
{
    std::vector<std::pair<String, String>> description;

    description.emplace_back("Type", toString(join_info.kind));
    description.emplace_back("Strictness", toString(join_info.strictness));
    description.emplace_back("Locality", toString(join_info.locality));

    {
        WriteBufferFromOwnString join_expression_str;
        join_expression_str << (join_info.expression.is_using ? "USING" : "ON") << " " ;
        formatJoinCondition(join_info.expression.condition, join_expression_str);
        for (const auto & condition : join_info.expression.disjunctive_conditions)
        {
            join_expression_str << " | ";
            formatJoinCondition(condition, join_expression_str);
        }
        description.emplace_back("Expression", join_expression_str.str());
    }

    description.emplace_back("Required Output", fmt::format("[{}]", fmt::join(required_output_columns, ", ")));

    for (const auto & [name, value] : runtime_info_description)
        description.emplace_back(name, value);

    return description;
}

void JoinStepLogical::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, settings.indent_char);

    for (const auto & [name, value] : describeJoinActions())
        settings.out << prefix << name << ": " << value << '\n';

    settings.out << prefix << "Post Expression:\n";
    ExpressionActions(expression_actions.post_join_actions->clone()).describeActions(settings.out, prefix);
    settings.out << prefix << "Left Expression:\n";
    ExpressionActions(expression_actions.left_pre_join_actions->clone()).describeActions(settings.out, prefix);
    settings.out << prefix << "Right Expression:\n";
    ExpressionActions(expression_actions.right_pre_join_actions->clone()).describeActions(settings.out, prefix);
}

void JoinStepLogical::describeActions(JSONBuilder::JSONMap & map) const
{
    for (const auto & [name, value] : describeJoinActions())
        map.add(name, value);

    map.add("Left Actions", ExpressionActions(expression_actions.left_pre_join_actions->clone()).toTree());
    map.add("Right Actions", ExpressionActions(expression_actions.right_pre_join_actions->clone()).toTree());
    map.add("Post Actions", ExpressionActions(expression_actions.post_join_actions->clone()).toTree());
}

static ActionsDAG::NodeRawConstPtrs getAnyColumn(const ActionsDAG::NodeRawConstPtrs & nodes)
{
    if (nodes.empty())
        throw Exception(ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK, "No columns in JOIN result");

    ActionsDAG::NodeRawConstPtrs result_nodes;
    for (const auto * output : nodes)
    {
        /// Add column as many times as it is used in output
        /// Otherwise, invariants in other parts of the code may be violated
        if (output->result_name == nodes.at(0)->result_name)
            result_nodes.push_back(output);
    }
    return result_nodes;
}

bool JoinStepLogical::canRemoveUnusedColumns() const
{
    return !hasDuplicatedNamesInInputOrOutputs(*expression_actions.post_join_actions)
        && !hasDuplicatedNamesInInputOrOutputs(*expression_actions.left_pre_join_actions)
        && !hasDuplicatedNamesInInputOrOutputs(*expression_actions.right_pre_join_actions);
}

IQueryPlanStep::UnusedColumnRemovalResult JoinStepLogical::removeUnusedColumns(NameMultiSet required_outputs, bool remove_inputs)
{
    {
        Names new_required_output_columns;
        new_required_output_columns.reserve(required_outputs.size());
        for (const auto * output : expression_actions.post_join_actions->getOutputs())
        {
            auto it = required_outputs.find(output->result_name);
            if (it == required_outputs.end())
                continue;
            new_required_output_columns.push_back(output->result_name);
            required_outputs.erase(it);
        }
        required_output_columns = std::move(new_required_output_columns);
    }

    NameSet left_required_outputs{};
    NameSet right_required_outputs{};

    auto collect_required_outputs_from_condition
        = [this, &left_required_outputs, &right_required_outputs](const JoinCondition & condition) mutable
    {
        for (const auto & predicate : condition.predicates)
        {
            left_required_outputs.insert(predicate.left_node.getColumnName());
            right_required_outputs.insert(predicate.right_node.getColumnName());
        }
        for (const auto & left_filter : condition.left_filter_conditions)
            left_required_outputs.insert(left_filter.getColumnName());
        for (const auto & right_filter : condition.right_filter_conditions)
            right_required_outputs.insert(right_filter.getColumnName());
        for (const auto & residual_condition : condition.residual_conditions)
            required_output_columns.push_back(residual_condition.getColumnName());
    };

    collect_required_outputs_from_condition(join_info.expression.condition);
    for (const auto & condition : join_info.expression.disjunctive_conditions)
        collect_required_outputs_from_condition(condition);

    if (required_output_columns.empty())
    {
        auto find_new_required_output_column = [&post_join_actions = expression_actions.post_join_actions](
                                                   const std::vector<JoinPredicate> & predicates) -> std::optional<String>
        {
            for (const auto & predicate : predicates)
            {
                if (const auto * output = post_join_actions->tryFindInOutputs(predicate.left_node.getColumnName()))
                    return output->result_name;
                if (const auto * output = post_join_actions->tryFindInOutputs(predicate.right_node.getColumnName()))
                    return output->result_name;
            }
            return std::nullopt;
        };
        // Prefer one of the predicates as predicates whenever it is possible, because those columns are needed for join and couldn't be removed in any case
        if (auto maybe_new_required_column_name = find_new_required_output_column(join_info.expression.condition.predicates))
            required_output_columns.push_back(std::move(*maybe_new_required_column_name));

        for (const auto & condition : join_info.expression.disjunctive_conditions)
            if (auto maybe_new_required_column_name = find_new_required_output_column(condition.predicates))
            {
                required_output_columns.push_back(std::move(*maybe_new_required_column_name));
                break;
            }

        // If we still didn't find a required column, let's pick one from one of the left/right required columns,
        // because they are necessary for the join, so let's just pass them through
        if (required_output_columns.empty())
        {
            const auto fill_required_output_columns_from = [&](const NameSet & names, ActionsDAG & actions_dag)
            {
                if (!names.empty())
                {
                    const auto & new_column_name = *names.begin();
                    const auto & output = actions_dag.findInOutputs(new_column_name);
                    required_output_columns.push_back(output.result_name);
                    const auto & new_output = expression_actions.post_join_actions->addInput(output.result_name, output.result_type);
                    expression_actions.post_join_actions->addOrReplaceInOutputs(new_output);
                }
            };

            if (!left_required_outputs.empty())
                fill_required_output_columns_from(left_required_outputs, *expression_actions.left_pre_join_actions);
            else if (!right_required_outputs.empty())
                fill_required_output_columns_from(right_required_outputs, *expression_actions.right_pre_join_actions);
        }

        // If we still don't have any (can this happen?) then let's pick a random column
    if (required_output_columns.empty())
        required_output_columns = calculateOutputHeader({})->getNames();
    }

    // We can always remove inputs from post join actions, that doesn't affect the inputs of this step
    bool removed_any_actions = expression_actions.post_join_actions->removeUnusedActions(required_output_columns);
    const auto & new_post_join_inputs = expression_actions.post_join_actions->getInputs();

    for (const auto * post_join_input : new_post_join_inputs)
    {
        if (const auto * left_output = expression_actions.left_pre_join_actions->tryFindInOutputs(post_join_input->result_name);
            nullptr != left_output)
            left_required_outputs.insert(post_join_input->result_name);
        else
            right_required_outputs.insert(post_join_input->result_name);
    }

    const auto add_one_column_from_outputs_if_no_required
        = [](NameSet & required_names, const ActionsDAG::NodeRawConstPtrs & previous_outputs)
    {
        if (!required_names.empty())
            return;

        chassert(!previous_outputs.empty());
        required_names.insert(previous_outputs.front()->result_name);
    };

    add_one_column_from_outputs_if_no_required(left_required_outputs, expression_actions.left_pre_join_actions->getOutputs());
    add_one_column_from_outputs_if_no_required(right_required_outputs, expression_actions.right_pre_join_actions->getOutputs());
    chassert(!left_required_outputs.empty());
    chassert(!right_required_outputs.empty());

    removed_any_actions |= expression_actions.left_pre_join_actions->removeUnusedActions(left_required_outputs, remove_inputs);
    removed_any_actions |= expression_actions.right_pre_join_actions->removeUnusedActions(right_required_outputs, remove_inputs);

    if (!removed_any_actions)
        return UnusedColumnRemovalResult{false, false};

    if (remove_inputs
        && (expression_actions.left_pre_join_actions->getInputs().size() < getInputHeaders().at(0)->columns()
            || expression_actions.right_pre_join_actions->getInputs().size() < getInputHeaders().at(1)->columns()))
    {
        SharedHeaders new_input_headers;

        const auto get_input_columns = [](const ActionsDAG & actions)
        {
            Block result;
            for (const auto * input_node : actions.getInputs())
                result.insert(ColumnWithTypeAndName{input_node->column, input_node->result_type, input_node->result_name});

            return std::make_shared<const Block>(std::move(result));
        };

        new_input_headers.push_back(get_input_columns(*expression_actions.left_pre_join_actions));
        new_input_headers.push_back(get_input_columns(*expression_actions.right_pre_join_actions));
        updateInputHeaders(std::move(new_input_headers));

        return UnusedColumnRemovalResult{true, true};
    }

    updateOutputHeader();

    return UnusedColumnRemovalResult{true, false};
}

bool JoinStepLogical::canRemoveColumnsFromOutput() const
{
    if (output_header == nullptr)
        return false;

    const auto minimal_output_header = calculateOutputHeader({});

    chassert(minimal_output_header->columns() <= output_header->columns());

    return canRemoveUnusedColumns() && !blocksHaveEqualStructure(*output_header, *minimal_output_header);
}


SharedHeader JoinStepLogical::calculateOutputHeader(const NameSet & required_output_columns_set) const
{
    Block header;
    for (const auto * node : expression_actions.post_join_actions->getInputs())
    {
        const auto & column_type = node->result_type;
        const auto & column_name = node->result_name;
        if (required_output_columns_set.contains(column_name))
            header.insert(ColumnWithTypeAndName(column_type->createColumn(), column_type, column_name));
    }

    if (header.empty())
    {
        for (const auto * node : getAnyColumn(expression_actions.post_join_actions->getInputs()))
        {
            const auto & column_type = node->result_type;
            const auto & column_name = node->result_name;
            header.insert(ColumnWithTypeAndName(column_type->createColumn(), column_type, column_name));
        }
    }

    return std::make_shared<const Block>(std::move(header));
}

void JoinStepLogical::updateOutputHeader()
{
    NameSet required_output_columns_set(required_output_columns.begin(), required_output_columns.end());
    output_header = calculateOutputHeader(required_output_columns_set);
}

JoinActionRef addNewOutput(const ActionsDAG::Node & node, ActionsDAGPtr & actions_dag)
{
    actions_dag->addOrReplaceInOutputs(node);
    return JoinActionRef(&node, actions_dag.get());
}

/// We may have expressions like `a and b` that work even if `a` and `b` are non-boolean and expression return boolean or nullable.
/// In some contexts, we may split `a` and `b`, but we still want to have the same logic applied, as if it were still an `and` operand.
JoinActionRef toBoolIfNeeded(JoinActionRef condition, ActionsDAG & actions_dag, const FunctionOverloadResolverPtr & concat_function)
{
    auto output_type = removeNullable(condition.getType());
    WhichDataType which_type(output_type);
    if (!which_type.isUInt8())
    {
        DataTypePtr uint8_ty = std::make_shared<DataTypeUInt8>();
        ColumnWithTypeAndName rhs;
        const ActionsDAG::Node * rhs_node = nullptr;
        if (concat_function->getName() == "and")
            rhs_node = &actions_dag.addColumn(ColumnWithTypeAndName(uint8_ty->createColumnConst(1, 1), uint8_ty, "true"));
        else if (concat_function->getName() == "or")
            rhs_node = &actions_dag.addColumn(ColumnWithTypeAndName(uint8_ty->createColumnConst(0, 0), uint8_ty, "false"));

        if (rhs_node)
        {
            rhs_node = &actions_dag.addFunction(concat_function, {condition.getNode(), rhs_node}, {});
            actions_dag.addOrReplaceInOutputs(*rhs_node);
            return JoinActionRef(rhs_node, &actions_dag);
        }
    }
    return condition;
}

JoinActionRef concatConditionsWithFunction(
    const std::vector<JoinActionRef> & conditions, const ActionsDAGPtr & actions_dag, const FunctionOverloadResolverPtr & concat_function)
{
    if (conditions.empty())
        return JoinActionRef(nullptr);

    if (conditions.size() == 1)
        return toBoolIfNeeded(conditions.front(), *actions_dag, concat_function);

    auto nodes = std::ranges::to<ActionsDAG::NodeRawConstPtrs>(std::views::transform(conditions, [](const auto & x) { return x.getNode(); }));

    const auto & result_node = actions_dag->addFunction(concat_function, nodes, {});
    actions_dag->addOrReplaceInOutputs(result_node);
    return JoinActionRef(&result_node, actions_dag.get());
}

JoinActionRef concatConditions(const std::vector<JoinActionRef> & conditions, const ActionsDAGPtr & actions_dag)
{
    FunctionOverloadResolverPtr and_function = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionAnd>());
    return concatConditionsWithFunction(conditions, actions_dag, and_function);
}

JoinActionRef concatMergeConditions(std::vector<JoinActionRef> & conditions, const ActionsDAGPtr & actions_dag)
{
    auto condition = concatConditions(conditions, actions_dag);
    conditions.clear();
    if (condition)
        conditions = {condition};
    return condition;
}

const ActionsDAG::Node & findOrAddInput(const ActionsDAGPtr & actions_dag, const ColumnWithTypeAndName & column)
{
    for (const auto * node : actions_dag->getInputs())
    {
        if (node->result_name == column.name)
            return *node;
    }

    return actions_dag->addInput(column);
}

JoinActionRef predicateToCondition(const JoinPredicate & predicate, const ActionsDAGPtr & actions_dag)
{
    ColumnWithTypeAndName left_column = predicate.left_node.getColumn();
    ColumnWithTypeAndName right_column = predicate.right_node.getColumn();

    /// Constant columns from the JOIN condition will be materialized during the JOIN,
    /// that's why we can't use them as constants for actions building.
    left_column.column = nullptr;
    right_column.column = nullptr;

    const auto & left_node = findOrAddInput(actions_dag, left_column);
    const auto & right_node = findOrAddInput(actions_dag, right_column);

    auto operator_function = FunctionFactory::instance().get(operatorToFunctionName(predicate.op), nullptr);
    const auto & result_node = actions_dag->addFunction(operator_function, {&left_node, &right_node}, {});
    actions_dag->addOrReplaceInOutputs(result_node);
    return JoinActionRef(&result_node, actions_dag.get());
}

bool canPushDownFromOn(const JoinInfo & join_info, std::optional<JoinTableSide> side = {})
{
    bool is_suitable_kind = join_info.kind == JoinKind::Inner
        || join_info.kind == JoinKind::Cross
        || join_info.kind == JoinKind::Comma
        || join_info.kind == JoinKind::Paste
        || (side == JoinTableSide::Left && join_info.kind == JoinKind::Right)
        || (side == JoinTableSide::Right && join_info.kind == JoinKind::Left);

    return is_suitable_kind
        && join_info.expression.disjunctive_conditions.empty()
        && join_info.strictness == JoinStrictness::All;
}

void addRequiredInputToOutput(const ActionsDAGPtr & dag, const NameSet & required_output_columns)
{
    NameSet existing_output_columns;
    auto & outputs = dag->getOutputs();
    for (const auto & node : outputs)
        existing_output_columns.insert(node->result_name);

    for (const auto * node : dag->getInputs())
    {
        if (!required_output_columns.contains(node->result_name)
         || existing_output_columns.contains(node->result_name))
            continue;
        outputs.push_back(node);
    }
}

struct JoinPlanningContext
{
    PreparedJoinStorage * prepared_join_storage = nullptr;
    bool is_asof = false;
    bool is_using = false;
};

void predicateOperandsToCommonType(JoinPredicate & predicate, JoinExpressionActions & expression_actions, JoinPlanningContext join_context)
{
    auto & left_node = predicate.left_node;
    auto & right_node = predicate.right_node;
    const auto & left_type = left_node.getType();
    const auto & right_type = right_node.getType();

    if (left_type->equals(*right_type))
        return;

    DataTypePtr common_type;
    try
    {
        common_type = getLeastSupertype(DataTypes{left_type, right_type});
    }
    catch (Exception & ex)
    {
        ex.addMessage("JOIN cannot infer common type in ON section for keys. Left key '{}' type {}. Right key '{}' type {}",
            left_node.getColumnName(), left_type->getName(),
            right_node.getColumnName(), right_type->getName());
        throw;
    }

    if (!left_type->equals(*common_type))
    {
        const auto & result_name = join_context.is_using ? left_node.getColumnName() : "";
        left_node = addNewOutput(
            expression_actions.left_pre_join_actions->addCast(*left_node.getNode(), common_type, result_name),
            expression_actions.left_pre_join_actions);
    }

    if (!right_type->equals(*common_type) && (!join_context.prepared_join_storage || join_context.prepared_join_storage->storage_key_value))
    {
        const std::string & result_name = join_context.is_using ? right_node.getColumnName() : "";
        right_node = addNewOutput(
            expression_actions.right_pre_join_actions->addCast(*right_node.getNode(), common_type, result_name),
            expression_actions.right_pre_join_actions);
    }
}

std::tuple<const ActionsDAG::Node *, const ActionsDAG::Node *> leftAndRightNodes(const JoinPredicate & predicate)
{
    return {predicate.left_node.getNode(), predicate.right_node.getNode()};
}

bool addJoinConditionToTableJoin(JoinCondition & join_condition, TableJoin::JoinOnClause & table_join_clause, JoinExpressionActions & expression_actions, JoinPlanningContext join_context)
{
    std::vector<JoinPredicate> new_predicates;
    for (size_t i = 0; i < join_condition.predicates.size(); ++i)
    {
        auto & predicate = join_condition.predicates[i];
        predicateOperandsToCommonType(predicate, expression_actions, join_context);
        if (PredicateOperator::Equals == predicate.op || PredicateOperator::NullSafeEquals == predicate.op)
        {
            auto [left_key_node, right_key_node] = leftAndRightNodes(predicate);
            bool null_safe_comparison = PredicateOperator::NullSafeEquals == predicate.op;
            if (null_safe_comparison && isNullableOrLowCardinalityNullable(left_key_node->result_type) && isNullableOrLowCardinalityNullable(right_key_node->result_type))
            {
                /**
                  * In case of null-safe comparison (a IS NOT DISTINCT FROM b),
                  * we need to wrap keys with a non-nullable type.
                  * The type `tuple` can be used for this purpose,
                  * because value tuple(NULL) is not NULL itself (moreover it has type Tuple(Nullable(T) which is not Nullable).
                  * Thus, join algorithm will match keys with values tuple(NULL).
                  * Example:
                  *   SELECT * FROM t1 JOIN t2 ON t1.a <=> t2.b
                  * This will be semantically transformed to:
                  *   SELECT * FROM t1 JOIN t2 ON tuple(t1.a) == tuple(t2.b)
                  */

                FunctionOverloadResolverPtr wrap_nullsafe_function = std::make_shared<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionTuple>());

                const auto & new_left_node = expression_actions.left_pre_join_actions->addFunction(wrap_nullsafe_function, {left_key_node}, {});
                expression_actions.left_pre_join_actions->addOrReplaceInOutputs(new_left_node);
                predicate.left_node = JoinActionRef(&new_left_node, expression_actions.left_pre_join_actions.get());

                const auto & new_right_node = expression_actions.right_pre_join_actions->addFunction(wrap_nullsafe_function, {right_key_node}, {});
                expression_actions.right_pre_join_actions->addOrReplaceInOutputs(new_right_node);
                predicate.right_node = JoinActionRef(&new_right_node, expression_actions.right_pre_join_actions.get());
            }

            table_join_clause.addKey(predicate.left_node.getColumnName(), predicate.right_node.getColumnName(), null_safe_comparison);
            new_predicates.push_back(predicate);
        }
        else if (join_context.is_asof)
        {
            /// ASOF preficate is handled later, keep it in predicate list
            new_predicates.push_back(predicate);
        }
        else
        {
            /// Move non-equality predicates to residual conditions
            auto predicate_action = predicateToCondition(predicate, expression_actions.post_join_actions);
            join_condition.residual_conditions.push_back(predicate_action);
        }
    }

    join_condition.predicates = std::move(new_predicates);

    return !join_condition.predicates.empty();
}

JoinActionRef buildSingleActionForJoinCondition(const JoinCondition & join_condition, JoinExpressionActions & expression_actions)
{
    std::vector<JoinActionRef> all_conditions;

    if (auto filter_condition = concatConditions(join_condition.left_filter_conditions, expression_actions.left_pre_join_actions))
    {
        const auto & node = findOrAddInput(expression_actions.post_join_actions, filter_condition.getColumn());
        expression_actions.post_join_actions->addOrReplaceInOutputs(node);
        all_conditions.emplace_back(&node, expression_actions.post_join_actions.get());
    }

    if (auto filter_condition = concatConditions(join_condition.right_filter_conditions, expression_actions.right_pre_join_actions))
    {
        const auto & node = findOrAddInput(expression_actions.post_join_actions, filter_condition.getColumn());
        expression_actions.post_join_actions->addOrReplaceInOutputs(node);
        all_conditions.emplace_back(&node, expression_actions.post_join_actions.get());
    }

    auto residual_conditions_action = concatConditions(join_condition.residual_conditions, expression_actions.post_join_actions);
    if (residual_conditions_action)
        all_conditions.push_back(residual_conditions_action);

    for (const auto & predicate : join_condition.predicates)
    {
        auto predicate_action = predicateToCondition(predicate, expression_actions.post_join_actions);
        all_conditions.push_back(predicate_action);
    }

    return concatConditions(all_conditions, expression_actions.post_join_actions);
}

JoinActionRef buildSingleActionForJoinExpression(const JoinExpression & join_expression, JoinExpressionActions & expression_actions)
{
    std::vector<JoinActionRef> all_conditions;

    if (auto condition = buildSingleActionForJoinCondition(join_expression.condition, expression_actions))
        all_conditions.push_back(condition);

    for (const auto & join_condition : join_expression.disjunctive_conditions)
        if (auto condition = buildSingleActionForJoinCondition(join_condition, expression_actions))
            all_conditions.push_back(condition);


    FunctionOverloadResolverPtr or_function = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionOr>());
    return concatConditionsWithFunction(all_conditions, expression_actions.post_join_actions, or_function);
}

void JoinStepLogical::setPreparedJoinStorage(PreparedJoinStorage storage) { prepared_join_storage = std::move(storage); }

static SharedHeader blockWithActionsDAGOutput(const ActionsDAG & actions_dag)
{
    ColumnsWithTypeAndName columns;
    columns.reserve(actions_dag.getOutputs().size());
    for (const auto & node : actions_dag.getOutputs())
        columns.emplace_back(node->column ? node->column : node->result_type->createColumn(), node->result_type, node->result_name);
    return std::make_shared<const Block>(Block{columns});
}

static void addToNullableActions(ActionsDAG & dag, const FunctionOverloadResolverPtr & to_nullable_function)
{
    for (auto & output_node : dag.getOutputs())
    {
        DataTypePtr type_to_check = output_node->result_type;
        if (const auto * type_to_check_low_cardinality = typeid_cast<const DataTypeLowCardinality *>(type_to_check.get()))
            type_to_check = type_to_check_low_cardinality->getDictionaryType();

        if (type_to_check->canBeInsideNullable())
            output_node = &dag.addFunction(to_nullable_function, {output_node}, output_node->result_name);
    }
}

void JoinStepLogical::appendRequiredOutputsToActions(JoinActionRef & post_filter)
{
    NameSet required_output_columns_set(required_output_columns.begin(), required_output_columns.end());
    addRequiredInputToOutput(expression_actions.left_pre_join_actions, required_output_columns_set);
    addRequiredInputToOutput(expression_actions.right_pre_join_actions, required_output_columns_set);
    addRequiredInputToOutput(expression_actions.post_join_actions, required_output_columns_set);

    ActionsDAG::NodeRawConstPtrs new_outputs;
    for (const auto * output : expression_actions.post_join_actions->getOutputs())
    {
        if (required_output_columns_set.contains(output->result_name))
            new_outputs.push_back(output);
    }

    if (new_outputs.empty())
    {
        new_outputs = getAnyColumn(expression_actions.post_join_actions->getOutputs());
    }

    if (post_filter)
        new_outputs.push_back(post_filter.getNode());
    expression_actions.post_join_actions->getOutputs() = std::move(new_outputs);
    expression_actions.post_join_actions->removeUnusedActions();
}

JoinPtr JoinStepLogical::convertToPhysical(
    JoinActionRef & post_filter,
    bool is_explain_logical,
    UInt64 max_threads,
    UInt64 max_entries_for_hash_table_stats,
    String initial_query_id,
    std::chrono::milliseconds lock_acquire_timeout,
    const ExpressionActionsSettings & actions_settings,
    std::optional<UInt64> rhs_size_estimation)
{
    auto table_join = std::make_shared<TableJoin>(join_settings, use_nulls,
        Context::getGlobalContextInstance()->getGlobalTemporaryVolume(),
        Context::getGlobalContextInstance()->getTempDataOnDisk());

    auto & join_expression = join_info.expression;

    JoinPlanningContext join_context;
    if (prepared_join_storage)
    {
        join_context.prepared_join_storage = &prepared_join_storage;
        prepared_join_storage.visit([&table_join](const auto & storage_)
        {
            table_join->setStorageJoin(storage_);
        });
        swap_inputs = false;
    }

    if (join_expression.is_using || join_info.kind == JoinKind::Paste)
        swap_inputs = false;

    join_context.is_asof = join_info.strictness == JoinStrictness::Asof;
    join_context.is_using = join_expression.is_using;

    auto & table_join_clauses = table_join->getClauses();

    if (!isCrossOrComma(join_info.kind) && !isPaste(join_info.kind))
    {
        bool has_keys = addJoinConditionToTableJoin(
            join_expression.condition, table_join_clauses.emplace_back(),
            expression_actions, join_context);

        if (!has_keys)
        {
            if (!TableJoin::isEnabledAlgorithm(join_settings.join_algorithms, JoinAlgorithm::HASH))
                throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION, "Cannot convert JOIN ON expression to CROSS JOIN, because hash join is disabled");

            table_join_clauses.pop_back();
            bool can_convert_to_cross = (isInner(join_info.kind) || isCrossOrComma(join_info.kind))
                && join_info.strictness == JoinStrictness::All
                && join_expression.disjunctive_conditions.empty()
                && join_expression.condition.left_filter_conditions.empty()
                && join_expression.condition.right_filter_conditions.empty();

            if (!can_convert_to_cross)
                throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION, "Cannot determine join keys in JOIN ON expression {}",
                    formatJoinCondition(join_expression.condition));
            join_info.kind = JoinKind::Cross;
        }
    }

    if (auto left_pre_filter_condition = concatMergeConditions(join_expression.condition.left_filter_conditions, expression_actions.left_pre_join_actions))
    {
        table_join_clauses.at(table_join_clauses.size() - 1).analyzer_left_filter_condition_column_name = left_pre_filter_condition.getColumnName();
    }

    if (auto right_pre_filter_condition = concatMergeConditions(join_expression.condition.right_filter_conditions, expression_actions.right_pre_join_actions))
    {
        table_join_clauses.at(table_join_clauses.size() - 1).analyzer_right_filter_condition_column_name = right_pre_filter_condition.getColumnName();
    }

    if (join_info.strictness == JoinStrictness::Asof)
    {
        if (!join_expression.disjunctive_conditions.empty())
            throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION, "ASOF join does not support multiple disjuncts in JOIN ON expression");

        /// Find strictly only one inequality in predicate list for ASOF join
        chassert(table_join_clauses.size() == 1);
        auto & join_predicates = join_expression.condition.predicates;
        bool asof_predicate_found = false;
        for (auto & predicate : join_predicates)
        {
            predicateOperandsToCommonType(predicate, expression_actions, join_context);
            auto asof_inequality_op = operatorToAsofInequality(predicate.op);
            if (!asof_inequality_op)
                continue;

            if (asof_predicate_found)
                throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION, "ASOF join does not support multiple inequality predicates in JOIN ON expression");
            asof_predicate_found = true;
            table_join->setAsofInequality(*asof_inequality_op);
            table_join_clauses.front().addKey(predicate.left_node.getColumnName(), predicate.right_node.getColumnName(), /* null_safe_comparison = */ false);
        }
        if (!asof_predicate_found)
            throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION, "ASOF join requires one inequality predicate in JOIN ON expression, in {}",
                formatJoinCondition(join_expression.condition));
    }

    for (auto & join_condition : join_expression.disjunctive_conditions)
    {
        auto & table_join_clause = table_join_clauses.emplace_back();
        bool has_keys = addJoinConditionToTableJoin(join_condition, table_join_clause, expression_actions, join_context);
        if (!has_keys)
            throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION, "Cannot determine join keys in JOIN ON expression {}",
                formatJoinCondition(join_condition));
        if (auto left_pre_filter_condition = concatMergeConditions(join_condition.left_filter_conditions, expression_actions.left_pre_join_actions))
            table_join_clause.analyzer_left_filter_condition_column_name = left_pre_filter_condition.getColumnName();
        if (auto right_pre_filter_condition = concatMergeConditions(join_condition.right_filter_conditions, expression_actions.right_pre_join_actions))
            table_join_clause.analyzer_right_filter_condition_column_name = right_pre_filter_condition.getColumnName();
    }

    JoinActionRef residual_filter_condition(nullptr);
    if (join_expression.disjunctive_conditions.empty())
    {
        residual_filter_condition = concatMergeConditions(
            join_expression.condition.residual_conditions, expression_actions.post_join_actions);
    }
    else
    {
        bool need_residual_filter = !join_expression.condition.residual_conditions.empty();
        for (const auto & join_condition : join_expression.disjunctive_conditions)
        {
            need_residual_filter = need_residual_filter || !join_condition.residual_conditions.empty();
            if (need_residual_filter)
                break;
        }

        if (need_residual_filter)
            residual_filter_condition = buildSingleActionForJoinExpression(join_expression, expression_actions);
    }

    bool need_add_nullable = join_info.kind == JoinKind::Left
        || join_info.kind == JoinKind::Right
        || join_info.kind == JoinKind::Full;
    if (need_add_nullable && use_nulls)
    {
        if (residual_filter_condition)
        {
            throw Exception(
                ErrorCodes::INVALID_JOIN_ON_EXPRESSION,
                "{} JOIN ON expression '{}' contains column from left and right table, which is not supported with `join_use_nulls`",
                toString(join_info.kind), residual_filter_condition.getColumnName());
        }

        auto to_nullable_function = FunctionFactory::instance().get("toNullable", nullptr);
        if (join_info.kind == JoinKind::Left || join_info.kind == JoinKind::Full)
            addToNullableActions(*expression_actions.right_pre_join_actions, to_nullable_function);
        if (join_info.kind == JoinKind::Right || join_info.kind == JoinKind::Full)
            addToNullableActions(*expression_actions.left_pre_join_actions, to_nullable_function);
    }

    if (residual_filter_condition && canPushDownFromOn(join_info))
    {
        post_filter = residual_filter_condition;
    }
    else if (residual_filter_condition)
    {
        ActionsDAG dag;
        if (is_explain_logical)
        {
            /// Keep post_join_actions for explain
            dag = expression_actions.post_join_actions->clone();
        }
        else
        {
            /// Move post_join_actions to join, replace with no-op dag
            dag = std::move(*expression_actions.post_join_actions);
            *expression_actions.post_join_actions = ActionsDAG(dag.getRequiredColumns());
        }
        auto & outputs = dag.getOutputs();
        for (const auto * node : outputs)
        {
            if (node->result_name == residual_filter_condition.getColumnName())
            {
                outputs = {node};
                break;
            }
        }
        ExpressionActionsPtr & mixed_join_expression = table_join->getMixedJoinExpression();
        mixed_join_expression = std::make_shared<ExpressionActions>(std::move(dag), actions_settings);
    }

    appendRequiredOutputsToActions(post_filter);

    table_join->setInputColumns(
        expression_actions.left_pre_join_actions->getNamesAndTypesList(),
        expression_actions.right_pre_join_actions->getNamesAndTypesList());
    table_join->setUsedColumns(expression_actions.post_join_actions->getRequiredColumnsNames());
    table_join->setJoinInfo(join_info);

    SharedHeader left_sample_block = blockWithActionsDAGOutput(*expression_actions.left_pre_join_actions);
    SharedHeader right_sample_block = blockWithActionsDAGOutput(*expression_actions.right_pre_join_actions);

    if (swap_inputs)
    {
        table_join->swapSides();
        std::swap(left_sample_block, right_sample_block);
        if (hash_table_key_hashes)
            std::swap(hash_table_key_hashes->key_hash_left, hash_table_key_hashes->key_hash_right);
    }

    JoinAlgorithmSettings algo_settings(
        join_settings,
        max_threads,
        max_entries_for_hash_table_stats,
        std::move(initial_query_id),
        lock_acquire_timeout);

    auto join_algorithm_ptr = chooseJoinAlgorithm(
        table_join,
        prepared_join_storage,
        left_sample_block,
        right_sample_block,
        algo_settings,
        hash_table_key_hashes ? hash_table_key_hashes->key_hash_right : 0,
        rhs_size_estimation);
    runtime_info_description.emplace_back("Algorithm", join_algorithm_ptr->getName());
    return join_algorithm_ptr;
}

bool JoinStepLogical::hasPreparedJoinStorage() const
{
    return prepared_join_storage;
}

std::optional<ActionsDAG> JoinStepLogical::getFilterActions(JoinTableSide side, String & filter_column_name)
{
    if (join_info.strictness != JoinStrictness::All)
        return {};

    auto & join_expression = join_info.expression;
    if (!join_expression.disjunctive_conditions.empty())
        return {};

    if (!canPushDownFromOn(join_info, side))
        return {};

    const ActionsDAGPtr & actions_dag = side == JoinTableSide::Left ? expression_actions.left_pre_join_actions : expression_actions.right_pre_join_actions;
    std::vector<JoinActionRef> & conditions = side == JoinTableSide::Left ? join_expression.condition.left_filter_conditions : join_expression.condition.right_filter_conditions;

    if (auto filter_condition = concatMergeConditions(conditions, actions_dag))
    {
        filter_column_name = filter_condition.getColumnName();
        conditions.clear();
        ActionsDAG new_dag(actions_dag->getResultColumns());
        new_dag.getOutputs() = new_dag.getInputs();

        ActionsDAG result = std::move(*actions_dag);
        *actions_dag = std::move(new_dag);

        updateInputHeader(std::make_shared<const Block>(result.getResultColumns()), side == JoinTableSide::Left ? 0 : 1);

        return result;
    }

    return {};
}

void JoinStepLogical::serializeSettings(QueryPlanSerializationSettings & settings) const
{
    join_settings.updatePlanSettings(settings);
    sorting_settings.updatePlanSettings(settings);
}

void JoinStepLogical::serialize(Serialization & ctx) const
{
    if (prepared_join_storage)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Serialization of JoinStepLogical with prepared storage is not implemented");

    if (swap_inputs)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Serialization of JoinStepLogical with swapped inputs is not implemented");

    UInt8 flags = 0;
    if (use_nulls)
        flags |= 1;

    writeIntBinary(flags, ctx.out);

    JoinActionRef::ActionsDAGRawPtrs dags = {
        expression_actions.left_pre_join_actions.get(),
        expression_actions.right_pre_join_actions.get(),
        expression_actions.post_join_actions.get()};

    writeVarUInt(dags.size(), ctx.out);
    for (const auto * dag : dags)
        dag->serialize(ctx.out, ctx.registry);

    join_info.serialize(ctx.out, dags);

    writeVarUInt(required_output_columns.size(), ctx.out);
    for (const auto & name : required_output_columns)
        writeStringBinary(name, ctx.out);
}

std::unique_ptr<IQueryPlanStep> JoinStepLogical::deserialize(Deserialization & ctx)
{
    if (ctx.input_headers.size() != 2)
        throw Exception(ErrorCodes::INCORRECT_DATA, "JoinStepLogical must have two input streams");

    UInt8 flags;
    readIntBinary(flags, ctx.in);

    bool use_nulls = flags & 1;

    std::vector<ActionsDAGPtr> dag_ptrs;
    {
        UInt64 num_dags;
        readVarUInt(num_dags, ctx.in);

        if (num_dags != 3)
            throw Exception(ErrorCodes::INCORRECT_DATA, "JoinStepLogical deserialization expect 3 DAGs, got {}", num_dags);

        dag_ptrs.reserve(num_dags);
        for (size_t i = 0; i < num_dags; ++i)
            dag_ptrs.emplace_back(std::make_unique<ActionsDAG>(ActionsDAG::deserialize(ctx.in, ctx.registry, ctx.context)));
    }

    JoinExpressionActions expression_actions(std::move(dag_ptrs[0]), std::move(dag_ptrs[1]), std::move(dag_ptrs[2]));

    JoinActionRef::ActionsDAGRawPtrs dags = {
        expression_actions.left_pre_join_actions.get(),
        expression_actions.right_pre_join_actions.get(),
        expression_actions.post_join_actions.get()};

    auto join_info = JoinInfo::deserialize(ctx.in, dags);

    Names required_output_columns;
    {
        UInt64 num_output_columns;
        readVarUInt(num_output_columns, ctx.in);

        required_output_columns.resize(num_output_columns);
        for (auto & name : required_output_columns)
            readStringBinary(name, ctx.in);
    }

    SortingStep::Settings sort_settings(ctx.settings);
    JoinSettings join_settings(ctx.settings);

    return std::make_unique<JoinStepLogical>(
        ctx.input_headers.front(), ctx.input_headers.back(),
        std::move(join_info),
        std::move(expression_actions),
        std::move(required_output_columns),
        use_nulls,
        std::move(join_settings),
        std::move(sort_settings));
}

QueryPlanStepPtr JoinStepLogical::clone() const
{
    auto new_expression_actions = expression_actions.clone();
    auto new_join_info = join_info.clone(new_expression_actions);

    auto result_step = std::make_unique<JoinStepLogical>(
        getInputHeaders().front(), getInputHeaders().back(),
        std::move(new_join_info),
        std::move(new_expression_actions),
        required_output_columns,
        use_nulls,
        join_settings,
        sorting_settings);
    result_step->setStepDescription(getStepDescription());
    return result_step;
}

void registerJoinStep(QueryPlanStepRegistry & registry)
{
    registry.registerStep("Join", JoinStepLogical::deserialize);
}

}
