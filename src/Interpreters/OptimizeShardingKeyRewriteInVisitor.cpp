#include <Interpreters/OptimizeShardingKeyRewriteInVisitor.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/Utils.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/ExpressionActions.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
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

/// Collect the names of the `IN` expressions that are reachable without going through a filtering
/// clause. `WHERE` may be the only place that computes a column which a later stage - `LIMIT BY`, for
/// example - then consumes by name, so rewriting the filter alone can still make the shard's block
/// disagree with the header the initiator expects.
void collectInNamesUsedOutsideFilters(const ASTPtr & node, std::unordered_set<String> & names)
{
    if (const auto * select = node->as<ASTSelectQuery>())
    {
        for (const auto & child : select->children)
        {
            if (child == select->where() || child == select->prewhere())
                continue;

            collectInNamesUsedOutsideFilters(child, names);
        }

        return;
    }

    if (const auto * function = node->as<ASTFunction>(); function && function->name == "in")
        names.insert(function->getColumnName());

    for (const auto & child : node->children)
        collectInNamesUsedOutsideFilters(child, names);
}

}

namespace DB
{

bool OptimizeShardingKeyRewriteInMatcher::needChildVisit(ASTPtr & node, const ASTPtr & child)
{
    /// Rewrite the set only inside the filtering clauses. Pruning the set to the elements routed to
    /// this shard leaves the value of the expression correct - a row on this shard can only equal an
    /// element routed here - but it changes the expression's name, and every other clause can carry
    /// that name into the header the shard returns to the initiator: the projection directly, and
    /// `GROUP BY` / `ORDER BY` / `LIMIT BY` through the intermediate stages, which ship the
    /// aggregation keys and the `before_order_by` columns and are matched by name on the initiator.
    ///
    /// The join tree is still visited, so a subquery in `FROM` keeps being pruned by its own filters.
    if (const auto * select = node->as<ASTSelectQuery>())
        return child == select->where() || child == select->prewhere() || child == select->tables();

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

    /// An aliased `IN` in a filter can be referenced from any other clause - and, in the old
    /// analyzer, the alias is expanded to a copy of this expression there. Rewriting only the copy
    /// in the filter would either rename the column the initiator binds, or leave two different
    /// expressions behind the same alias (`MULTIPLE_EXPRESSIONS_FOR_ALIAS`).
    if (!function.tryGetAlias().empty())
        return;

    /// The same expression is used outside the filters, where its name reaches the initiator.
    if (data.in_names_used_outside_filters && data.in_names_used_outside_filters->contains(function.getColumnName()))
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

    /// See the comment in `OptimizeShardingKeyRewriteInMatcher::needChildVisit`: outside of the
    /// filtering clauses the rewrite keeps the value of the expression but changes its name, and the
    /// initiator binds the columns the shard returns by name.
    static bool needChildVisit(QueryTreeNodePtr & parent, QueryTreeNodePtr & child)
    {
        if (const auto * query_node = parent->as<QueryNode>())
            return child == query_node->getWhere() || child == query_node->getPrewhere() || child == query_node->getJoinTreeNode();

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

void optimizeShardingKeyRewriteIn(ASTPtr & query, OptimizeShardingKeyRewriteInMatcher::Data data)
{
    std::unordered_set<String> in_names_used_outside_filters;
    collectInNamesUsedOutsideFilters(query, in_names_used_outside_filters);
    data.in_names_used_outside_filters = &in_names_used_outside_filters;

    OptimizeShardingKeyRewriteInVisitor visitor(data);
    visitor.visit(query);
}

}
