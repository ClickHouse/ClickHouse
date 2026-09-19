#include <Interpreters/OptimizeShardingKeyRewriteInVisitor.h>

#include <Analyzer/ArrayJoinNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/Utils.h>
#include <DataTypes/DataTypesNumber.h>
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

}

namespace DB
{

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

    /// Rewrite the set only inside the filtering clauses. Pruning the set to the elements routed to
    /// this shard leaves the value of the expression correct - a row on this shard can only equal an
    /// element routed here - but it changes the expression's name, and every other clause can carry
    /// that name into the header the shard returns to the initiator: the projection directly, and
    /// `GROUP BY` / `ORDER BY` / `LIMIT BY` through the intermediate stages, which ship the
    /// aggregation keys and the `before_order_by` columns and are matched by name on the initiator.
    ///
    /// The join tree is visited too, so a subquery in `FROM` keeps being pruned by its own filters,
    /// but only its table expressions are: the `ON` section of a `JOIN` and the expressions of an
    /// `ARRAY JOIN` belong to a different source, which is not partitioned by the sharding key of this
    /// table even when its columns share their names with it, and the arguments of a table function
    /// are not part of the query stage that is executed on the shard.
    static bool needChildVisit(QueryTreeNodePtr & parent, QueryTreeNodePtr & child)
    {
        if (const auto * query_node = parent->as<QueryNode>())
            return child == query_node->getWhere() || child == query_node->getPrewhere() || child == query_node->getJoinTreeNode();

        if (const auto * join_node = parent->as<JoinNode>())
            return child == join_node->getLeftTableExpressionNode() || child == join_node->getRightTableExpressionNode();

        if (const auto * array_join_node = parent->as<ArrayJoinNode>())
            return child == array_join_node->getTableExpressionNode();

        if (parent->as<TableFunctionNode>())
            return false;

        return true;
    }

    void enterImpl(QueryTreeNodePtr & node)
    {
        auto * function_node = node->as<FunctionNode>();
        if (!function_node || function_node->getFunctionName() != "in")
            return;

        /// An aliased node is shared between the clauses that reference the alias, so rewriting it
        /// through a filter would also rewrite it in the projection or in `ORDER BY`.
        if (node->hasAlias())
            return;

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
                /// `IN ()` with an empty constant tuple is a degenerate query that the
                /// AST-side rewriter (above) also skips via `tuple.size() > 1`. Bail out
                /// before `tuple.back()` below to avoid UB on an empty `Tuple`.
                if (tuple.empty())
                    return;

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

    OptimizeShardingKeyRewriteInVisitor::Data data;
};

void optimizeShardingKeyRewriteIn(QueryTreeNodePtr & node, OptimizeShardingKeyRewriteInVisitor::Data data, ContextPtr context)
{
    OptimizeShardingKeyRewriteIn visitor(std::move(data), std::move(context));
    visitor.visit(node);
}

}
