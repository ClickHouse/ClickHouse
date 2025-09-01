#include <Interpreters/OptimizeShardingKeyRewriteInVisitor.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/Utils.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/ExpressionActions.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/IAST_erase.h>

namespace
{

using namespace DB;

Field executeFunctionOnField(
    const Field & field,
    const std::string & name,
    const ExpressionActionsPtr & sharding_expr,
    const DataTypePtr & type,
    const std::string & sharding_key_column_name)
{
    ColumnWithTypeAndName column;
    column.column = type->createColumnConst(1, field);
    column.name = name;
    column.type = type;

    Block block{column};
    size_t num_rows = 1;
    sharding_expr->execute(block, num_rows);

    ColumnWithTypeAndName & ret = block.getByName(sharding_key_column_name);
    return (*ret.column)[0];
}

/// @param column_value - one of values from IN
/// @param sharding_column_name - name of that column
/// @return true if shard may contain such value (or it is unknown), otherwise false.
bool shardContains(
    Field column_value,
    const std::string & sharding_column_name,
    const OptimizeShardingKeyRewriteInMatcher::Data & data)
{
    /// Type of column in storage (used for implicit conversion from i.e. String to Int)
    const DataTypePtr & column_type = data.sharding_key_expr->getSampleBlock().getByName(sharding_column_name).type;
    /// Implicit conversion.
    column_value = convertFieldToType(column_value, *column_type);

    /// NULL is not allowed in sharding key,
    /// so it should be safe to assume that shard cannot contain it.
    if (column_value.isNull())
        return false;

    Field sharding_value = executeFunctionOnField(
        column_value, sharding_column_name,
        data.sharding_key_expr, column_type,
        data.sharding_key_column_name);
    /// The value from IN can be non-numeric,
    /// but in this case it should be convertible to numeric type, let's try.
    ///
    /// NOTE: that conversion should not be done for signed types,
    /// since it uses accurate cast, that will return Null,
    /// but we need static_cast<> (as createBlockSelector()).
    if (!isInt64OrUInt64FieldType(sharding_value.getType()))
        sharding_value = convertFieldToType(sharding_value, DataTypeUInt64());
    /// In case of conversion is not possible (NULL), shard cannot contain the value anyway.
    if (sharding_value.isNull())
        return false;

    UInt64 value = sharding_value.safeGet<UInt64>();
    const auto shard_num = data.slots[value % data.slots.size()] + 1;
    return data.shard_info.shard_num == shard_num;
}

/// Helper function used for sharding key expression DAG conversion into query tree node.
///
/// @param dag_node - actions to be converted
/// @param allowed_inputs - column names and corresponding nodes, used for actions DAG inputs conversion
/// @return query tree node representation of sharding key actions DAG
QueryTreeNodePtr actionsDAGNodeToQueryTreeNode(
    const ActionsDAG::Node & dag_node,
    const std::map<std::string, QueryTreeNodePtr> & allowed_inputs)
{
    switch (dag_node.type)
    {
        case ActionsDAG::ActionType::INPUT: {
            auto it = allowed_inputs.find(dag_node.result_name);
            if (it == allowed_inputs.end())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Input {} is not is allowed list", dag_node.result_name);
            return it->second;
        }
        case ActionsDAG::ActionType::COLUMN: {
            if (!dag_node.column || !isColumnConst(*dag_node.column))
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported conversion of non-constant COLUMN action");
            return std::make_shared<ConstantNode>((*dag_node.column)[0]);
        }
        case ActionsDAG::ActionType::FUNCTION: {
            auto function_node = std::make_shared<FunctionNode>(dag_node.function_base->getName());
            auto & arguments = function_node->getArguments().getNodes();
            for (const auto * child : dag_node.children)
                arguments.push_back(actionsDAGNodeToQueryTreeNode(*child, allowed_inputs));
            return function_node;
        }
        default:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported action type in actionsDAGNodeToQueryTreeNode: {}", dag_node.type);
    }
}

}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

bool OptimizeShardingKeyRewriteInMatcher::needChildVisit(ASTPtr & /*node*/, const ASTPtr & /*child*/)
{
    return true;
}

void OptimizeShardingKeyRewriteInMatcher::visit(ASTPtr & node, Data & data)
{
    if (auto * function = node->as<ASTFunction>())
        visit(*function, data);
}

void OptimizeShardingKeyRewriteInMatcher::visit(ASTFunction & function, Data & data)
{
    if (function.name != "in")
        return;

    auto * left = function.arguments->children.front().get();
    auto * right = function.arguments->children.back().get();
    auto * identifier = left->as<ASTIdentifier>();
    if (!identifier)
        return;

    auto name = identifier->shortName();
    if (!data.sharding_key_expr->getRequiredColumnsWithTypes().contains(name))
        return;

    if (auto * tuple_func = right->as<ASTFunction>(); tuple_func && tuple_func->name == "tuple")
    {
        auto * tuple_elements = tuple_func->children.front()->as<ASTExpressionList>();
        std::erase_if(tuple_elements->children, [&](auto & child)
        {
            auto * literal = child->template as<ASTLiteral>();
            return tuple_elements->children.size() > 1 && literal && !shardContains(literal->value, name, data);
        });
    }
    else if (auto * tuple_literal = right->as<ASTLiteral>();
        tuple_literal && tuple_literal->value.getType() == Field::Types::Tuple)
    {
        auto & tuple = tuple_literal->value.safeGet<Tuple>();
        if (tuple.size() > 1)
        {
            Tuple new_tuple;

            for (auto & child : tuple)
                if (shardContains(child, name, data))
                    new_tuple.emplace_back(std::move(child));

            if (new_tuple.empty())
                new_tuple.emplace_back(std::move(tuple.back()));

            tuple_literal->value = std::move(new_tuple);
        }
    }
}


class OptimizeShardingKeyRewriteIn : public InDepthQueryTreeVisitorWithContext<OptimizeShardingKeyRewriteIn>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<OptimizeShardingKeyRewriteIn>;

    OptimizeShardingKeyRewriteIn(OptimizeShardingKeyRewriteInVisitor::Data data_, ContextPtr context)
        : Base(std::move(context))
        , data(std::move(data_))
    {}

    void enterImpl(QueryTreeNodePtr & node)
    {
        auto * function_node = node->as<FunctionNode>();
        if (!function_node)
            return;

        if (function_node->getFunctionName() == "in" && data.allow_rewrite_in)
            rewriteInPredicate(function_node);
        else if (function_node->getFunctionName() == "globalIn" && data.allow_rewrite_global_in)
            rewriteGlobalInPredicate(function_node);
    }

    void rewriteInPredicate(FunctionNode * function_node)
    {
        auto & arguments = function_node->getArguments().getNodes();
        auto * column = arguments[0]->as<ColumnNode>();
        if (!column)
            return;

        auto name = column->getColumnName();

        if (!data.sharding_key_expr->getRequiredColumnsWithTypes().contains(column->getColumnName()))
            return;

        if (auto * constant = arguments[1]->as<ConstantNode>())
        {
            if (isTuple(constant->getResultType()))
            {
                const auto tuple = constant->getValue().safeGet<Tuple>();
                Tuple new_tuple;
                new_tuple.reserve(tuple.size());

                for (const auto & child : tuple)
                {
                    if (shardContains(child, name, data))
                        new_tuple.push_back(child);
                }

                if (new_tuple.empty())
                    new_tuple.push_back(tuple.back());

                if (new_tuple.size() == tuple.size())
                    return;

                arguments[1] = std::make_shared<ConstantNode>(new_tuple);
                rerunFunctionResolve(function_node, getContext());
            }
        }
    }

    void rewriteGlobalInPredicate(FunctionNode * function_node)
    {
        auto & arguments = function_node->getArguments().getNodes();

        const auto & required_columns = data.sharding_key_expr->getRequiredColumns();
        if (required_columns.size() != 1)
            return;

        const auto & required_column_name = required_columns.front();

        auto * column_in = arguments[0]->as<ColumnNode>();
        if (!column_in || column_in->getColumnName() != required_column_name)
            return;

        const auto & source_table = arguments[1];
        if (!source_table->as<TableNode>())
            return;

        const auto * sharding_key_expr_node = data.sharding_key_expr->getActionsDAG().tryFindInOutputs(data.sharding_key_column_name);
        if (!sharding_key_expr_node)
            return;

        const auto & subquery = buildSubqueryToReadColumnsFromTableExpression(source_table, getContext());
        const auto & subquery_column = subquery->as<QueryNode &>().getProjection().getNodes().front();

        std::map<std::string, QueryTreeNodePtr> allowed_inputs;
        allowed_inputs[required_column_name] = subquery_column;

        QueryTreeNodePtr sharding_key_filter;
        try
        {
            sharding_key_filter = actionsDAGNodeToQueryTreeNode(*sharding_key_expr_node, allowed_inputs);
        }
        catch (const Exception & e)
        {
            if (e.code() == ErrorCodes::NOT_IMPLEMENTED)
                return;

            throw;
        }

        const auto target_shard_expr = std::make_shared<FunctionNode>("modulo");
        target_shard_expr->getArguments().getNodes().push_back(sharding_key_filter);
        target_shard_expr->getArguments().getNodes().push_back(std::make_shared<ConstantNode>(data.slots.size()));

        const auto shard_filter_expr = std::make_shared<FunctionNode>("equals");
        shard_filter_expr->getArguments().getNodes().push_back(target_shard_expr);
        shard_filter_expr->getArguments().getNodes().push_back(std::make_shared<ConstantNode>(data.shard_info.shard_num - 1));

        subquery->as<QueryNode &>().getWhere() = shard_filter_expr;

        arguments[1] = subquery;
        rerunFunctionResolve(function_node, getContext());
    }

    OptimizeShardingKeyRewriteInVisitor::Data data;
};

void optimizeShardingKeyRewriteIn(QueryTreeNodePtr & node, OptimizeShardingKeyRewriteInVisitor::Data data, ContextPtr context)
{
    OptimizeShardingKeyRewriteIn visitor(std::move(data), std::move(context));
    visitor.visit(node);
}

}
