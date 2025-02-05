#include <Planner/PlannerJoinsLogical.h>
#include <Planner/PlannerJoins.h>

#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>

#include <DataTypes/getLeastSupertype.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>

#include <Storages/IStorage.h>
#include <Storages/StorageJoin.h>
#include <Storages/StorageDictionary.h>

#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>

#include <Analyzer/Utils.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/JoinNode.h>

#include <Dictionaries/IDictionary.h>
#include <Interpreters/IKeyValueEntity.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Interpreters/MergeJoin.h>
#include <Interpreters/FullSortingMergeJoin.h>
#include <Interpreters/ConcurrentHashJoin.h>
#include <Interpreters/DirectJoin.h>
#include <Interpreters/JoinSwitcher.h>
#include <Interpreters/ArrayJoinAction.h>
#include <Interpreters/GraceHashJoin.h>
#include <Interpreters/PasteJoin.h>

#include <Planner/PlannerActionsVisitor.h>
#include <Planner/PlannerContext.h>
#include <Planner/Utils.h>
#include <Processors/QueryPlan/JoinStepLogical.h>

#include <Core/Settings.h>
#include <Core/ServerSettings.h>
#include <Interpreters/JoinInfo.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INVALID_JOIN_ON_EXPRESSION;
}

namespace Setting
{
    extern const SettingsBool join_use_nulls;
}

const ActionsDAG::Node * appendExpression(
    ActionsDAG & dag,
    const QueryTreeNodePtr & expression,
    const PlannerContextPtr & planner_context)
{
    PlannerActionsVisitor join_expression_visitor(planner_context);
    auto join_expression_dag_node_raw_pointers = join_expression_visitor.visit(dag, expression);
    if (join_expression_dag_node_raw_pointers.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Expression {} expected be a single node, got {}",
            expression->formatASTForErrorMessage(), dag.dumpDAG());

    dag.addOrReplaceInOutputs(*join_expression_dag_node_raw_pointers[0]);
    return join_expression_dag_node_raw_pointers[0];
}

struct JoinInfoBuildContext
{
    static ColumnsWithTypeAndName combineJoinedColumns(
        const ColumnsWithTypeAndName & left_table_columns_,
        const ColumnsWithTypeAndName & right_table_columns_,
        const JoinNode & join_node_,
        const PlannerContextPtr & planner_context_)
    {
        ColumnsWithTypeAndName joined_columns;
        std::unordered_map<std::string_view, DataTypePtr> changed_types;
        if (join_node_.isUsingJoinExpression())
        {
            auto & using_list = join_node_.getJoinExpression()->as<ListNode &>();
            for (auto & join_using_node : using_list.getNodes())
            {
                auto & column_node = join_using_node->as<ColumnNode &>();
                auto & column_node_sources = column_node.getExpressionOrThrow()->as<ListNode &>();
                changed_types.emplace(
                    planner_context_->getColumnNodeIdentifierOrThrow(column_node_sources.getNodes().at(0)),
                    column_node.getColumnType());
                changed_types.emplace(
                    planner_context_->getColumnNodeIdentifierOrThrow(column_node_sources.getNodes().at(1)),
                    column_node.getColumnType());
            }
        }

        bool join_use_nulls = planner_context_->getQueryContext()->getSettingsRef()[Setting::join_use_nulls];
        auto join_kind = join_node_.getKind();
        if (join_use_nulls)
        {
            bool convert_left = join_kind == JoinKind::Right || join_kind == JoinKind::Full;
            bool convert_right = join_kind == JoinKind::Left || join_kind == JoinKind::Full;
            for (const auto * table_columns : {&left_table_columns_, &right_table_columns_})
            {
                if ((!convert_left && table_columns == &left_table_columns_)
                 || (!convert_right && table_columns == &right_table_columns_))
                    continue;

                for (const auto & column : *table_columns)
                {
                    auto it = changed_types.find(column.name);
                    if (it != changed_types.end())
                        it->second = JoinCommon::convertTypeToNullable(it->second);
                    else
                        changed_types.emplace(column.name, JoinCommon::convertTypeToNullable(column.type));
                }
            }
        }

        for (const auto * table_columns : {&left_table_columns_, &right_table_columns_})
        {
            for (const auto & column : *table_columns)
            {
                auto it = changed_types.find(column.name);
                if (it != changed_types.end())
                    joined_columns.push_back(ColumnWithTypeAndName(it->second->createColumn(), it->second, column.name));
                else
                    joined_columns.push_back(ColumnWithTypeAndName(column.column->convertToFullColumnIfConst(), column.type, column.name));
            }
        }

        return joined_columns;
    }

    explicit JoinInfoBuildContext(
        const JoinNode & join_node_,
        const ColumnsWithTypeAndName & left_table_columns_,
        const ColumnsWithTypeAndName & right_table_columns_,
        const PlannerContextPtr & planner_context_)
        : join_node(join_node_)
        , planner_context(planner_context_)
        , left_table_columns(left_table_columns_)
        , right_table_columns(right_table_columns_)
        , left_table_expression_set(extractTableExpressionsSet(join_node.getLeftTableExpression()))
        , right_table_expression_set(extractTableExpressionsSet(join_node.getRightTableExpression()))
        , result_join_expression_actions(
            left_table_columns,
            right_table_columns,
            combineJoinedColumns(left_table_columns, right_table_columns, join_node, planner_context))
    {
        result_join_info.kind = join_node.getKind();
        result_join_info.strictness = join_node.getStrictness();
        result_join_info.locality = join_node.getLocality();
    }

    enum class JoinSource : uint8_t { None, Left, Right, Both };

    JoinSource getExpressionSource(const QueryTreeNodePtr & node)
    {
        auto res = extractJoinTableSidesFromExpression(node.get(), left_table_expression_set, right_table_expression_set, join_node);
        if (res.empty())
            return JoinSource::None;
        if (res.size() == 1)
        {
            if (*res.begin() == JoinTableSide::Left)
                return JoinSource::Left;
            return JoinSource::Right;
        }
        return JoinSource::Both;
    }

    JoinActionRef addExpression(const QueryTreeNodePtr & node, JoinSource src)
    {
        const ActionsDAG::Node * dag_node_ptr = nullptr;
        if (src == JoinSource::Left)
            dag_node_ptr = appendExpression(result_join_expression_actions.left_pre_join_actions, node, planner_context);
        else if (src == JoinSource::Right)
            dag_node_ptr = appendExpression(result_join_expression_actions.right_pre_join_actions, node, planner_context);
        else
            dag_node_ptr = appendExpression(result_join_expression_actions.post_join_actions, node, planner_context);
        return JoinActionRef(dag_node_ptr);
    }

    const JoinNode & join_node;
    const PlannerContextPtr & planner_context;

    ColumnsWithTypeAndName left_table_columns;
    ColumnsWithTypeAndName right_table_columns;

    TableExpressionSet left_table_expression_set;
    TableExpressionSet right_table_expression_set;

    JoinExpressionActions result_join_expression_actions;
    JoinInfo result_join_info;
};

bool tryGetJoinPredicate(const FunctionNode * function_node, JoinInfoBuildContext & builder_context, JoinCondition & join_condition)
{
    if (!function_node || function_node->getArguments().getNodes().size() != 2)
        return false;

    auto predicate_operator = getJoinPredicateOperator(function_node->getFunctionName());
    if (!predicate_operator.has_value())
        return false;

    auto left_node = function_node->getArguments().getNodes().at(0);
    auto left_expr_source = builder_context.getExpressionSource(left_node);

    auto right_node = function_node->getArguments().getNodes().at(1);
    auto right_expr_source = builder_context.getExpressionSource(right_node);

    if (left_expr_source == JoinInfoBuildContext::JoinSource::Left && right_expr_source == JoinInfoBuildContext::JoinSource::Right)
    {
        join_condition.predicates.emplace_back(JoinPredicate{
            builder_context.addExpression(left_node, JoinInfoBuildContext::JoinSource::Left),
            builder_context.addExpression(right_node, JoinInfoBuildContext::JoinSource::Right),
            predicate_operator.value()});
        return true;
    }

    if (left_expr_source == JoinInfoBuildContext::JoinSource::Right && right_expr_source == JoinInfoBuildContext::JoinSource::Left)
    {
        join_condition.predicates.push_back(JoinPredicate{
            builder_context.addExpression(right_node, JoinInfoBuildContext::JoinSource::Left),
            builder_context.addExpression(left_node, JoinInfoBuildContext::JoinSource::Right),
            reversePredicateOperator(predicate_operator.value())});
        return true;
    }

    return false;
}

void buildJoinUsingCondition(const QueryTreeNodePtr & node, JoinInfoBuildContext & builder_context, JoinCondition & join_condition)
{
    auto & using_list = node->as<ListNode &>();
    for (auto & using_node : using_list.getNodes())
    {
        auto & using_column_node = using_node->as<ColumnNode &>();
        auto & inner_columns_list = using_column_node.getExpressionOrThrow()->as<ListNode &>();
        chassert(inner_columns_list.getNodes().size() == 2);

        join_condition.predicates.emplace_back(JoinPredicate{
            builder_context.addExpression(inner_columns_list.getNodes().at(0), JoinInfoBuildContext::JoinSource::Left),
            builder_context.addExpression(inner_columns_list.getNodes().at(1), JoinInfoBuildContext::JoinSource::Right),
            PredicateOperator::Equals});
    }

    /// For ASOF join, the last column in USING list is the ASOF column
    if (builder_context.result_join_info.strictness == JoinStrictness::Asof && !join_condition.predicates.empty())
        join_condition.predicates.back().op = PredicateOperator::GreaterOrEquals;
}

void buildJoinCondition(const QueryTreeNodePtr & node, JoinInfoBuildContext & builder_context, JoinCondition & join_condition)
{
    std::string function_name;
    const auto * function_node = node->as<FunctionNode>();
    if (function_node)
        function_name = function_node->getFunction()->getName();

    if (function_name == "and")
    {
        for (const auto & child : function_node->getArguments())
            buildJoinCondition(child, builder_context, join_condition);
        return;
    }

    bool is_predicate = tryGetJoinPredicate(function_node, builder_context, join_condition);
    if (is_predicate)
        return;

    auto expr_source = builder_context.getExpressionSource(node);
    if (expr_source == JoinInfoBuildContext::JoinSource::Left || expr_source == JoinInfoBuildContext::JoinSource::None)
        join_condition.left_filter_conditions.push_back(builder_context.addExpression(node, JoinInfoBuildContext::JoinSource::Left));
    else if (expr_source == JoinInfoBuildContext::JoinSource::Right)
        join_condition.right_filter_conditions.push_back(builder_context.addExpression(node, JoinInfoBuildContext::JoinSource::Right));
    else
        join_condition.residual_conditions.push_back(builder_context.addExpression(node, expr_source));
}

void buildDisjunctiveJoinConditions(const QueryTreeNodePtr & node, JoinInfoBuildContext & builder_context, std::vector<JoinCondition> & join_conditions)
{
    auto * function_node = node->as<FunctionNode>();
    if (!function_node)
        throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION,
            "JOIN {} join expression expected function",
            node->formatASTForErrorMessage());

    const auto & function_name = function_node->getFunction()->getName();

    if (function_name == "or")
    {
        for (const auto & child : function_node->getArguments())
            buildDisjunctiveJoinConditions(child, builder_context, join_conditions);
        return;
    }
    buildJoinCondition(node, builder_context, join_conditions.emplace_back());
}

std::unique_ptr<JoinStepLogical> buildJoinStepLogical(
    const Block & left_header,
    const Block & right_header,
    const NameSet & outer_scope_columns,
    const JoinNode & join_node,
    const PlannerContextPtr & planner_context)
{
    const auto & left_columns = left_header.getColumnsWithTypeAndName();
    const auto & right_columns = right_header.getColumnsWithTypeAndName();
    JoinInfoBuildContext build_context(join_node, left_columns, right_columns, planner_context);

    const auto & join_on_expression = join_node.getJoinExpression();
    auto join_expression_constant = tryExtractConstantFromConditionNode(join_on_expression);
    if (join_on_expression && join_on_expression->getNodeType() == QueryTreeNodeType::CONSTANT && !join_expression_constant.has_value())
    {
        throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION, "Wrong type {} of JOIN expression {}",
            join_on_expression->getResultType()->getName(), join_on_expression->formatASTForErrorMessage());
    }

    auto join_expression_node = getJoinExpressionFromNode(join_node);

    /// CROSS/PASTE JOIN: doesn't have expression
    if (join_expression_node == nullptr)
    {
        if (!isCrossOrComma(join_node.getKind()) && !isPaste(join_node.getKind()))
            throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION, "Missing join expression in {}", join_node.formatASTForErrorMessage());
    }
    /// USING
    else if (join_node.isUsingJoinExpression())
    {
        buildJoinUsingCondition(join_expression_node, build_context, build_context.result_join_info.expression.condition);
        build_context.result_join_info.expression.is_using = true;
    }
    /// JOIN ON non-constant expression
    else if (!join_expression_constant.has_value() || build_context.result_join_info.strictness == JoinStrictness::Asof)
    {
        if (join_expression_node->getNodeType() != QueryTreeNodeType::FUNCTION)
            throw Exception(ErrorCodes::INVALID_JOIN_ON_EXPRESSION,
                "JOIN {} join expression expected function",
                join_node.formatASTForErrorMessage());

        buildDisjunctiveJoinConditions(join_expression_node, build_context, build_context.result_join_info.expression.disjunctive_conditions);
        if (!build_context.result_join_info.expression.disjunctive_conditions.empty())
        {
            build_context.result_join_info.expression.condition = build_context.result_join_info.expression.disjunctive_conditions.back();
            build_context.result_join_info.expression.disjunctive_conditions.pop_back();
        }
    }
    else if (join_expression_constant.has_value())
    {
        auto & join_actions = build_context.result_join_expression_actions;

        /// Joined table expression always has __tableN prefix, other columns will appear only in projections, so these names are safe
        if (join_actions.left_pre_join_actions.tryFindInOutputs("__lhs_const"))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected reserved name '__lhs_const' in JOIN expression {}", join_actions.left_pre_join_actions.dumpDAG());
        if (join_actions.right_pre_join_actions.tryFindInOutputs("__rhs_const"))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected reserved name '__rhs_const' in JOIN expression {}", join_actions.right_pre_join_actions.dumpDAG());

        bool join_expression_value = join_expression_constant.value();
        auto dt = std::make_shared<DataTypeUInt8>();
        JoinActionRef lhs_node(&join_actions.left_pre_join_actions.addColumn(
            ColumnWithTypeAndName(dt->createColumnConstWithDefaultValue(1), dt, "__lhs_const")));
        join_actions.left_pre_join_actions.addOrReplaceInOutputs(*lhs_node.node);

        JoinActionRef rhs_node(&join_actions.right_pre_join_actions.addColumn(
            ColumnWithTypeAndName(dt->createColumnConst(1, join_expression_value ? 0 : 1), dt, "__rhs_const")));
        join_actions.right_pre_join_actions.addOrReplaceInOutputs(*rhs_node.node);

        build_context.result_join_info.expression.condition.predicates.emplace_back(JoinPredicate{lhs_node, rhs_node, PredicateOperator::Equals});
    }

    return std::make_unique<JoinStepLogical>(
        left_header,
        right_header,
        std::move(build_context.result_join_info),
        std::move(build_context.result_join_expression_actions),
        Names(outer_scope_columns.begin(), outer_scope_columns.end()),
        planner_context->getQueryContext());
}

PreparedJoinStorage tryGetStorageInTableJoin(const QueryTreeNodePtr & table_expression, const PlannerContextPtr & planner_context)
{
    StoragePtr storage;

    if (auto * table_node = table_expression->as<TableNode>())
        storage = table_node->getStorage();
    else if (auto * table_function = table_expression->as<TableFunctionNode>())
        storage = table_function->getStorage();
    else
        return {};

    PreparedJoinStorage result;
    const auto & table_expression_data = planner_context->getTableExpressionDataOrThrow(table_expression);
    result.column_mapping = table_expression_data.getColumnIdentifierToColumnName();

    result.storage_join = std::dynamic_pointer_cast<StorageJoin>(storage);
    if (result.storage_join)
        return result;

    auto storage_dictionary = std::dynamic_pointer_cast<StorageDictionary>(storage);
    if (storage_dictionary && storage_dictionary->getDictionary()->getSpecialKeyType() != DictionarySpecialKeyType::Range)
    {
        result.storage_key_value = std::dynamic_pointer_cast<const IKeyValueEntity>(storage_dictionary->getDictionary());
        return result;
    }

    result.storage_key_value = std::dynamic_pointer_cast<IKeyValueEntity>(storage);
    if (result.storage_key_value)
        return result;

    return {};
}

}
