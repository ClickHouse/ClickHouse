#include <Processors/QueryPlan/JoinStepLogical.h>

#include <Processors/QueryPlan/JoinStep.h>

#include <algorithm>
#include <ranges>
#include <Core/Settings.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <IO/Operators.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/PasteJoin.h>
#include <Interpreters/TableJoin.h>
#include <Planner/PlannerJoins.h>
#include <Processors/Transforms/JoiningTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/StorageJoin.h>
#include <Common/JSONBuilder.h>
#include <Common/typeid_cast.h>

namespace DB
{

namespace Setting
{
    extern const SettingsJoinAlgorithm join_algorithm;
    extern const SettingsBool join_any_take_last_row;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int INVALID_JOIN_ON_EXPRESSION;
    extern const int NOT_FOUND_COLUMN_IN_BLOCK;
}


std::string toFunctionName(PredicateOperator op)
{
    switch (op)
    {
        case PredicateOperator::Equals: return "equals";
        case PredicateOperator::NullSafeEquals: return "isNotDistinctFrom";
        case PredicateOperator::Less: return "less";
        case PredicateOperator::LessOrEquals: return "lessOrEquals";
        case PredicateOperator::Greater: return "greater";
        case PredicateOperator::GreaterOrEquals: return "greaterOrEquals";
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
    auto quote_string = std::views::transform([](const auto & s) { return fmt::format("({})", s.column_name); });
    auto format_predicate = std::views::transform([](const auto & p) { return fmt::format("{} {} {}", p.left_node.column_name, toString(p.op), p.right_node.column_name); });
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

std::vector<std::pair<String, String>> describeJoinActions(const JoinInfo & join_info)
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

    return description;
}


JoinStepLogical::JoinStepLogical(
    const Block & left_header_,
    const Block & right_header_,
    JoinInfo join_info_,
    JoinExpressionActions join_expression_actions_,
    Names required_output_columns_,
    ContextPtr context_)
    : expression_actions(std::move(join_expression_actions_))
    , join_info(std::move(join_info_))
    , required_output_columns(std::move(required_output_columns_))
    , query_context(std::move(context_))
    , join_settings(JoinSettings::create(query_context->getSettingsRef()))
    , sorting_settings(*query_context)
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

void JoinStepLogical::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, settings.indent_char);

    for (const auto & [name, value] : describeJoinActions(join_info))
        settings.out << prefix << name << ": " << value << '\n';
    for (const auto & [name, value] : runtime_info_description)
        settings.out << prefix << name << ": " << value << '\n';
    settings.out << prefix << "Post Expression:\n";
    ExpressionActions(expression_actions.post_join_actions.clone()).describeActions(settings.out, prefix);
    settings.out << prefix << "Left Expression:\n";
    // settings.out << expression_actions.left_pre_join_actions.dumpDAG();
    ExpressionActions(expression_actions.left_pre_join_actions.clone()).describeActions(settings.out, prefix);
    settings.out << prefix << "Right Expression:\n";
    ExpressionActions(expression_actions.right_pre_join_actions.clone()).describeActions(settings.out, prefix);
}

void JoinStepLogical::describeActions(JSONBuilder::JSONMap & map) const
{
    for (const auto & [name, value] : describeJoinActions(join_info))
        map.add(name, value);

    map.add("Left Actions", ExpressionActions(expression_actions.left_pre_join_actions.clone()).toTree());
    map.add("Right Actions", ExpressionActions(expression_actions.right_pre_join_actions.clone()).toTree());
    map.add("Post Actions", ExpressionActions(expression_actions.post_join_actions.clone()).toTree());
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

IQueryPlanStep::UnusedColumnRemovalResult JoinStepLogical::removeUnusedColumns(const Names & required_outputs, bool remove_inputs)
{
    required_output_columns = required_outputs;
    Names left_required_outputs{};
    Names right_required_outputs{};

    auto collect_required_outputs_from_condition
        = [this, &left_required_outputs, &right_required_outputs](const JoinCondition & condition) mutable
    {
        for (const auto & predicate : condition.predicates)
        {
            left_required_outputs.push_back(predicate.left_node.column_name);
            right_required_outputs.push_back(predicate.right_node.column_name);
        }
        for (const auto & residual_condition : condition.residual_conditions)
            required_output_columns.push_back(residual_condition.column_name);
    };

    collect_required_outputs_from_condition(join_info.expression.condition);
    for (const auto & condition : join_info.expression.disjunctive_conditions)
        collect_required_outputs_from_condition(condition);

    // Join needs to have at least one output and calculateOutputHeader will produce a deterministic result
    if (required_output_columns.empty())
        required_output_columns = calculateOutputHeader({}).getNames();

    // We can always remove inputs from post join actions, that doesn't affect the inputs of this step
    bool removed_any_actions = expression_actions.post_join_actions.removeUnusedActions(required_output_columns);

    // TODO(antaljanosbenjamin): can it happen that post join actions needs some output from any of the prejoin actions that are not in a JoinCondition?
    removed_any_actions |= expression_actions.left_pre_join_actions.removeUnusedActions(left_required_outputs, remove_inputs);
    removed_any_actions |= expression_actions.right_pre_join_actions.removeUnusedActions(right_required_outputs, remove_inputs);

    if (!removed_any_actions)
        return UnusedColumnRemovalResult{false, false};

    if (remove_inputs
        && (expression_actions.left_pre_join_actions.getInputs().size() < getInputHeaders().at(0).columns()
            || expression_actions.right_pre_join_actions.getInputs().size() < getInputHeaders().at(1).columns()))
    {
        Headers new_input_headers;

        const auto get_input_columns = [](const ActionsDAG & actions)
        {
            Header result;
            for (const auto * input_node : actions.getInputs())
                result.insert(ColumnWithTypeAndName{input_node->column, input_node->result_type, input_node->result_name});

            return result;
        };

        new_input_headers.push_back(get_input_columns(expression_actions.left_pre_join_actions));
        new_input_headers.push_back(get_input_columns(expression_actions.right_pre_join_actions));
        updateInputHeaders(std::move(new_input_headers));

        return UnusedColumnRemovalResult{true, true};
    }

    updateOutputHeader();

    return UnusedColumnRemovalResult{true, false};
}

bool JoinStepLogical::canRemoveColumnsFromOutput() const
{
    if (!output_header.has_value())
        return false;

    const auto minimal_output_header = calculateOutputHeader({});

    chassert(minimal_output_header.columns() <= output_header->columns());

    return !blocksHaveEqualStructure(*output_header, minimal_output_header);
}


Header JoinStepLogical::calculateOutputHeader(const NameSet & required_output_columns_set) const
{
    Header header;
    for (const auto * node : expression_actions.post_join_actions.getInputs())
    {
        const auto & column_type = node->result_type;
        const auto & column_name = node->result_name;
        if (required_output_columns_set.contains(column_name))
            header.insert(ColumnWithTypeAndName(column_type->createColumn(), column_type, column_name));
    }

    if (!header)
    {
        for (const auto * node : getAnyColumn(expression_actions.post_join_actions.getInputs()))
        {
            const auto & column_type = node->result_type;
            const auto & column_name = node->result_name;
            header.insert(ColumnWithTypeAndName(column_type->createColumn(), column_type, column_name));
        }
    }

    return header;
}

void JoinStepLogical::updateOutputHeader()
{
    NameSet required_output_columns_set(required_output_columns.begin(), required_output_columns.end());
    output_header = calculateOutputHeader(required_output_columns_set);
}

/// We may have expressions like `a and b` that work even if `a` and `b` are non-boolean and expression return boolean or nullable.
/// In some contexts, we may split `a` and `b`, but we still want to have the same logic applied, as if it were still an `and` operand.
JoinActionRef toBoolIfNeeded(JoinActionRef condition, ActionsDAG & actions_dag, const FunctionOverloadResolverPtr & concat_function)
{
    auto output_type = removeNullable(condition.node->result_type);
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
            return JoinActionRef(&actions_dag.addFunction(concat_function, {condition.node, rhs_node}, {}));
    }
    return condition;
}

JoinActionRef concatConditions(const std::vector<JoinActionRef> & conditions, ActionsDAG & actions_dag, const FunctionOverloadResolverPtr & concat_function)
{
    if (conditions.empty())
        return JoinActionRef(nullptr);

    if (conditions.size() == 1)
    {
        auto result = toBoolIfNeeded(conditions.front(), actions_dag, concat_function);
        actions_dag.addOrReplaceInOutputs(*result.node);
        return result;
    }

    ActionsDAG::NodeRawConstPtrs nodes;
    nodes.reserve(conditions.size());
    for (const auto & condition : conditions)
    {
        if (!condition.node)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Condition node is nullptr");
        nodes.push_back(condition.node);
    }

    const auto & result_node = actions_dag.addFunction(concat_function, nodes, {});
    actions_dag.addOrReplaceInOutputs(result_node);
    return JoinActionRef(&result_node);
}

JoinActionRef concatConditions(const std::vector<JoinActionRef> & conditions, ActionsDAG & actions_dag, const ContextPtr & query_context)
{
    auto and_function = FunctionFactory::instance().get("and", query_context);
    return concatConditions(conditions, actions_dag, and_function);
}

JoinActionRef concatMergeConditions(std::vector<JoinActionRef> & conditions, ActionsDAG & actions_dag, const ContextPtr & query_context)
{
    auto condition = concatConditions(conditions, actions_dag, query_context);
    conditions.clear();
    if (condition)
        conditions = {condition};
    return condition;
}

/// Can be used when action.node is outside of actions_dag.
const ActionsDAG::Node & findInput(ActionsDAG & actions_dag, const JoinActionRef & action, bool allow_add = false)
{
    for (const auto * node : actions_dag.getInputs())
    {
        if (node->result_name == action.column_name)
            return *node;
    }

    if (allow_add)
    {
        const auto & node = actions_dag.addInput(action.column_name, action.node->result_type);
        return node;
    }

    throw Exception(ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK, "Column {} not found in actions DAG: {}", action.column_name, actions_dag.dumpDAG());
}


JoinActionRef predicateToCondition(const JoinPredicate & predicate, ActionsDAG & actions_dag, const ContextPtr & query_context)
{
    const auto & left_node = findInput(actions_dag, predicate.left_node, true);
    const auto & right_node = findInput(actions_dag, predicate.right_node, true);

    auto operator_function = FunctionFactory::instance().get(toFunctionName(predicate.op), query_context);
    const auto & result_node = actions_dag.addFunction(operator_function, {&left_node, &right_node}, {});
    return JoinActionRef(&result_node);
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

void addRequiredInputToOutput(ActionsDAG & dag, const NameSet & required_output_columns)
{
    NameSet existing_output_columns;
    for (const auto & node : dag.getOutputs())
        existing_output_columns.insert(node->result_name);

    for (const auto * node : dag.getInputs())
    {
        if (!required_output_columns.contains(node->result_name)
         || existing_output_columns.contains(node->result_name))
            continue;
        dag.addOrReplaceInOutputs(*node);
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
    const auto & left_type = left_node.node->result_type;
    const auto & right_type = right_node.node->result_type;

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
            left_node.column_name, left_type->getName(),
            right_node.column_name, right_type->getName());
        throw;
    }

    if (!left_type->equals(*common_type))
    {
        const std::string & result_name = join_context.is_using ? left_node.column_name : "";
        left_node = JoinActionRef(&expression_actions.left_pre_join_actions.addCast(*left_node.node, common_type, result_name));
        expression_actions.left_pre_join_actions.addOrReplaceInOutputs(*left_node.node);
    }

    if (!join_context.prepared_join_storage && !right_type->equals(*common_type))
    {
        const std::string & result_name = join_context.is_using ? right_node.column_name : "";
        right_node = JoinActionRef(&expression_actions.right_pre_join_actions.addCast(*right_node.node, common_type, result_name));
        expression_actions.right_pre_join_actions.addOrReplaceInOutputs(*right_node.node);
    }
}

std::tuple<const ActionsDAG::Node *, const ActionsDAG::Node *> leftAndRightNodes(const JoinPredicate & predicate)
{
    return {predicate.left_node.node, predicate.right_node.node};
}

bool addJoinConditionToTableJoin(JoinCondition & join_condition, TableJoin::JoinOnClause & table_join_clause, JoinExpressionActions & expression_actions, const ContextPtr & query_context, JoinPlanningContext join_context)
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
                auto wrap_nullsafe_function = FunctionFactory::instance().get("tuple", query_context);
                predicate.left_node = JoinActionRef(&expression_actions.left_pre_join_actions.addFunction(wrap_nullsafe_function, {left_key_node}, {}));
                predicate.right_node = JoinActionRef(&expression_actions.right_pre_join_actions.addFunction(wrap_nullsafe_function, {right_key_node}, {}));

                expression_actions.left_pre_join_actions.addOrReplaceInOutputs(*predicate.left_node.node);
                expression_actions.right_pre_join_actions.addOrReplaceInOutputs(*predicate.right_node.node);
            }

            table_join_clause.addKey(predicate.left_node.column_name, predicate.right_node.column_name, null_safe_comparison);
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
            auto predicate_action = predicateToCondition(predicate, expression_actions.post_join_actions, query_context);
            join_condition.residual_conditions.push_back(predicate_action);
        }
    }

    join_condition.predicates = std::move(new_predicates);

    return !join_condition.predicates.empty();
}

JoinActionRef buildSingleActionForJoinCondition(const JoinCondition & join_condition, JoinExpressionActions & expression_actions, const ContextPtr & query_context)
{
    std::vector<JoinActionRef> all_conditions;
    auto left_filter_conditions_action = concatConditions(join_condition.left_filter_conditions, expression_actions.left_pre_join_actions, query_context);
    if (left_filter_conditions_action)
    {
        left_filter_conditions_action.node = &findInput(expression_actions.post_join_actions, left_filter_conditions_action, true);
        all_conditions.push_back(left_filter_conditions_action);
    }

    auto right_filter_conditions_action = concatConditions(join_condition.right_filter_conditions, expression_actions.right_pre_join_actions, query_context);
    if (right_filter_conditions_action)
    {
        right_filter_conditions_action.node = &findInput(expression_actions.post_join_actions, right_filter_conditions_action, true);
        all_conditions.push_back(right_filter_conditions_action);
    }

    auto residual_conditions_action = concatConditions(join_condition.residual_conditions, expression_actions.post_join_actions, query_context);
    if (residual_conditions_action)
        all_conditions.push_back(residual_conditions_action);

    for (const auto & predicate : join_condition.predicates)
    {
        auto predicate_action = predicateToCondition(predicate, expression_actions.post_join_actions, query_context);
        all_conditions.push_back(predicate_action);
    }

    return concatConditions(all_conditions, expression_actions.post_join_actions, query_context);
}

JoinActionRef buildSingleActionForJoinExpression(const JoinExpression & join_expression, JoinExpressionActions & expression_actions, const ContextPtr & query_context)
{
    std::vector<JoinActionRef> all_conditions;

    if (auto condition = buildSingleActionForJoinCondition(join_expression.condition, expression_actions, query_context))
        all_conditions.push_back(condition);

    for (const auto & join_condition : join_expression.disjunctive_conditions)
        if (auto condition = buildSingleActionForJoinCondition(join_condition, expression_actions, query_context))
            all_conditions.push_back(condition);

    auto or_function = FunctionFactory::instance().get("or", query_context);
    return concatConditions(all_conditions, expression_actions.post_join_actions, or_function);
}


void JoinStepLogical::setHashTableCacheKey(IQueryTreeNode::HashState hash_table_key_hash_) { hash_table_key_hash = std::move(hash_table_key_hash_); }
void JoinStepLogical::setPreparedJoinStorage(PreparedJoinStorage storage) { prepared_join_storage = std::move(storage); }

static Block blockWithColumns(ColumnsWithTypeAndName columns)
{
    Block block;
    for (const auto & column : columns)
        block.insert(ColumnWithTypeAndName(column.column ? column.column : column.type->createColumn(), column.type, column.name));
    return block;
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

JoinPtr JoinStepLogical::convertToPhysical(JoinActionRef & left_filter, JoinActionRef & right_filter, JoinActionRef & post_filter, bool is_explain_logical)
{
    const auto & settings = query_context->getSettingsRef();

    auto table_join = std::make_shared<TableJoin>(settings, query_context->getGlobalTemporaryVolume(), query_context->getTempDataOnDisk());

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

    if (join_expression.is_using)
        swap_inputs = false;

    join_context.is_asof = join_info.strictness == JoinStrictness::Asof;
    join_context.is_using = join_expression.is_using;

    auto & table_join_clauses = table_join->getClauses();

    if (!isCrossOrComma(join_info.kind) && !isPaste(join_info.kind))
    {
        bool has_keys = addJoinConditionToTableJoin(
            join_expression.condition, table_join_clauses.emplace_back(),
            expression_actions, query_context, join_context);

        if (!has_keys)
        {
            if (!TableJoin::isEnabledAlgorithm(join_settings.join_algorithm, JoinAlgorithm::HASH))
                throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION, "Cannot convert JOIN ON expression to CROSS JOIN, because hash join is disabled");

            table_join_clauses.pop_back();
            bool can_convert_to_cross = (isInner(join_info.kind) || isCrossOrComma(join_info.kind))
                && join_info.strictness == JoinStrictness::All
                && join_expression.disjunctive_conditions.empty();

            if (!can_convert_to_cross)
                throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION, "Cannot determine join keys in JOIN ON expression {}",
                    formatJoinCondition(join_expression.condition));
            join_info.kind = JoinKind::Cross;
        }
    }

    if (auto left_pre_filter_condition = concatMergeConditions(join_expression.condition.left_filter_conditions, expression_actions.left_pre_join_actions, query_context))
    {
        if (canPushDownFromOn(join_info, JoinTableSide::Left))
            left_filter = left_pre_filter_condition;
        else
            table_join_clauses.back().analyzer_left_filter_condition_column_name = left_pre_filter_condition.column_name;
    }

    if (auto right_pre_filter_condition = concatMergeConditions(join_expression.condition.right_filter_conditions, expression_actions.right_pre_join_actions, query_context))
    {
        if (canPushDownFromOn(join_info, JoinTableSide::Right))
            right_filter = right_pre_filter_condition;
        else
            table_join_clauses.back().analyzer_right_filter_condition_column_name = right_pre_filter_condition.column_name;
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
            table_join_clauses.front().addKey(predicate.left_node.column_name, predicate.right_node.column_name, /* null_safe_comparison = */ false);
        }
        if (!asof_predicate_found)
            throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION, "ASOF join requires one inequality predicate in JOIN ON expression, in {}",
                formatJoinCondition(join_expression.condition));
    }

    for (auto & join_condition : join_expression.disjunctive_conditions)
    {
        auto & table_join_clause = table_join_clauses.emplace_back();
        addJoinConditionToTableJoin(join_condition, table_join_clause, expression_actions, query_context, join_context);
        if (auto left_pre_filter_condition = concatMergeConditions(join_condition.left_filter_conditions, expression_actions.left_pre_join_actions, query_context))
            table_join_clause.analyzer_left_filter_condition_column_name = left_pre_filter_condition.column_name;
        if (auto right_pre_filter_condition = concatMergeConditions(join_condition.right_filter_conditions, expression_actions.right_pre_join_actions, query_context))
            table_join_clause.analyzer_right_filter_condition_column_name = right_pre_filter_condition.column_name;
    }

    JoinActionRef residual_filter_condition(nullptr);
    if (join_expression.disjunctive_conditions.empty())
    {
        residual_filter_condition = concatMergeConditions(
            join_expression.condition.residual_conditions, expression_actions.post_join_actions, query_context);
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
            residual_filter_condition = buildSingleActionForJoinExpression(join_expression, expression_actions, query_context);
    }

    bool need_add_nullable = join_info.kind == JoinKind::Left
        || join_info.kind == JoinKind::Right
        || join_info.kind == JoinKind::Full;
    if (need_add_nullable && join_settings.join_use_nulls)
    {
        if (residual_filter_condition)
        {
            throw Exception(
                ErrorCodes::INVALID_JOIN_ON_EXPRESSION,
                "{} JOIN ON expression '{}' contains column from left and right table, which is not supported with `join_use_nulls`",
                toString(join_info.kind), residual_filter_condition.column_name);
        }

        auto to_nullable_function = FunctionFactory::instance().get("toNullable", query_context);
        if (join_info.kind == JoinKind::Left || join_info.kind == JoinKind::Full)
            addToNullableActions(expression_actions.right_pre_join_actions, to_nullable_function);
        if (join_info.kind == JoinKind::Right || join_info.kind == JoinKind::Full)
            addToNullableActions(expression_actions.left_pre_join_actions, to_nullable_function);
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
            dag = expression_actions.post_join_actions.clone();
        }
        else
        {
            /// Move post_join_actions to join, replace with no-op dag
            dag = std::move(expression_actions.post_join_actions);
            expression_actions.post_join_actions = ActionsDAG(dag.getRequiredColumns());
        }
        auto & outputs = dag.getOutputs();
        for (const auto * node : outputs)
        {
            if (node->result_name == residual_filter_condition.column_name)
            {
                outputs = {node};
                break;
            }
        }
        ExpressionActionsPtr & mixed_join_expression = table_join->getMixedJoinExpression();
        mixed_join_expression = std::make_shared<ExpressionActions>(std::move(dag), ExpressionActionsSettings(query_context));
    }

    NameSet required_output_columns_set(required_output_columns.begin(), required_output_columns.end());
    addRequiredInputToOutput(expression_actions.left_pre_join_actions, required_output_columns_set);
    addRequiredInputToOutput(expression_actions.right_pre_join_actions, required_output_columns_set);
    addRequiredInputToOutput(expression_actions.post_join_actions, required_output_columns_set);

    ActionsDAG::NodeRawConstPtrs new_outputs;
    for (const auto * output : expression_actions.post_join_actions.getOutputs())
    {
        if (required_output_columns_set.contains(output->result_name))
            new_outputs.push_back(output);
    }

    if (new_outputs.empty())
    {
        new_outputs = getAnyColumn(expression_actions.post_join_actions.getOutputs());
    }

    if (post_filter)
        new_outputs.push_back(post_filter.node);
    expression_actions.post_join_actions.getOutputs() = std::move(new_outputs);
    expression_actions.post_join_actions.removeUnusedActions();

    table_join->setInputColumns(
        expression_actions.left_pre_join_actions.getNamesAndTypesList(),
        expression_actions.right_pre_join_actions.getNamesAndTypesList());
    table_join->setUsedColumns(expression_actions.post_join_actions.getRequiredColumnsNames());
    table_join->setJoinInfo(join_info);

    Block left_sample_block = blockWithColumns(expression_actions.left_pre_join_actions.getResultColumns());
    Block right_sample_block = blockWithColumns(expression_actions.right_pre_join_actions.getResultColumns());

    if (swap_inputs)
    {
        table_join->swapSides();
        std::swap(left_sample_block, right_sample_block);
    }

    auto join_algorithm_ptr = chooseJoinAlgorithm(
        table_join,
        prepared_join_storage,
        left_sample_block,
        right_sample_block,
        query_context,
        hash_table_key_hash);
    runtime_info_description.emplace_back("Algorithm", join_algorithm_ptr->getName());
    return join_algorithm_ptr;
}

bool JoinStepLogical::hasPreparedJoinStorage() const
{
    return prepared_join_storage;
}

}
