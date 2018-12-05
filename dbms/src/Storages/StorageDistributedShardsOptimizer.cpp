#include <Storages/StorageDistributed.h>

#include <DataTypes/DataTypesNumber.h>
#include <Common/typeid_cast.h>

#include <Interpreters/convertFieldToType.h>
#include <Interpreters/createBlockSelector.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
}

namespace
{
/// Contains a list of columns for conjunction: columns[0] AND columns[1] AND ...
struct Conjunction
{
    ColumnsWithTypeAndName columns;
};

/// Contains a list of disjunctions: disjunctions[0] OR disjunctions[1] OR ...
struct Disjunction
{
    std::vector<Conjunction> conjunctions;
};

using Disjunctions = std::vector<Disjunction>;
using DisjunctionsPtr = std::shared_ptr<Disjunctions>;

static constexpr auto and_function_name = "and";
static constexpr auto equals_function_name = "equals";
static constexpr auto in_function_name = "in";
static constexpr auto or_function_name = "or";
static constexpr auto tuple_function_name = "tuple";

void logDebug(std::string message)
{
    LOG_DEBUG(&Logger::get("(StorageDistributedShardsOptimizer)"), message);
}

/// Returns disjunction equivalent to `disjunctions AND another`.
Disjunctions pairwiseAnd(const Disjunctions & disjunctions, const Disjunctions & another)
{
    Disjunctions new_disjunctions;

    if (disjunctions.empty())
    {
        return another;
    }

    if (another.empty())
    {
        return disjunctions;
    }

    for (const auto disjunction : disjunctions)
    {
        for (const auto another_disjunction : another)
        {
            std::vector<Conjunction> new_conjunctions;

            for (const auto conjunction : disjunction.conjunctions)
            {
                for (const auto another_conjunction : another_disjunction.conjunctions)
                {
                    ColumnsWithTypeAndName new_columns;
                    new_columns.insert(std::end(new_columns), conjunction.columns.begin(), conjunction.columns.end());
                    new_columns.insert(std::end(new_columns), another_conjunction.columns.begin(), another_conjunction.columns.end());

                    new_conjunctions.push_back(Conjunction{new_columns});
                }
            }

            new_disjunctions.push_back(Disjunction{new_conjunctions});
        }
    }

    return new_disjunctions;
}

/// Given `ident = literal` expr, returns disjunctions relevant for constant folding in sharding_key_expr.
DisjunctionsPtr analyzeEquals(const ASTIdentifier * ident, const ASTLiteral * literal, ExpressionActionsPtr sharding_key_expr)
{
    for (const auto name_and_type : sharding_key_expr->getRequiredColumnsWithTypes())
    {
        const auto type = name_and_type.type;
        const auto name = name_and_type.name;

        if (name == ident->name)
        {
            ColumnWithTypeAndName column;

            column.column = type->createColumnConst(1, convertFieldToType(literal->value, *type));
            column.type = type;
            column.name = name;

            const auto columns = ColumnsWithTypeAndName{column};
            const auto conjunction = Conjunction{columns};
            const auto disjunction = Disjunction{{conjunction}};
            const Disjunctions disjunctions = {disjunction};

            return std::make_shared<Disjunctions>(disjunctions);
        }
    }

    const Disjunctions disjunctions = {};
    return std::make_shared<Disjunctions>(disjunctions);
}

/// Given `ident IN (..literals)` expr, returns disjunctions relevant for constant folding in sharding_key_expr.
DisjunctionsPtr analyzeIn(
    const ASTIdentifier * ident, const std::vector<const ASTLiteral *> literals, ExpressionActionsPtr sharding_key_expr)
{
    Disjunctions disjunctions;

    for (const auto literal : literals)
    {
        const auto inner_disjunctions = analyzeEquals(ident, literal, sharding_key_expr);

        if (!inner_disjunctions)
            return nullptr;

        disjunctions.insert(std::end(disjunctions), inner_disjunctions->begin(), inner_disjunctions->end());
    }

    return std::make_shared<Disjunctions>(disjunctions);
}

/// Given WHERE condition, returns disjunctions relevant for constant folding in sharding_key_expr.
DisjunctionsPtr analyzeQuery(const ASTFunction * function, ExpressionActionsPtr sharding_key_expr)
{
    if (function->name == equals_function_name)
    {
        auto left_arg = function->arguments->children.front().get();
        auto right_arg = function->arguments->children.back().get();

        // try to ensure left_arg points to ASTIdentifier
        if (!typeid_cast<const ASTIdentifier *>(left_arg) && typeid_cast<const ASTIdentifier *>(right_arg))
            std::swap(left_arg, right_arg);

        const auto ident = typeid_cast<const ASTIdentifier *>(left_arg);
        const auto literal = typeid_cast<const ASTLiteral *>(right_arg);

        if (!ident || !literal)
        {
            logDebug("didn't match pattern ident = <literal>");
            return nullptr;
        }

        return analyzeEquals(ident, literal, sharding_key_expr);
    }
    else if (function->name == in_function_name)
    {
        const auto left_arg = function->arguments->children.front().get();
        const auto right_arg = function->arguments->children.back().get();

        const auto ident = typeid_cast<const ASTIdentifier *>(left_arg);
        const auto inner_function = typeid_cast<const ASTFunction *>(right_arg);

        if (!ident || !inner_function || inner_function->name != tuple_function_name)
        {
            logDebug("didn't match pattern ident IN tuple(...)");
            return nullptr;
        }

        std::vector<const ASTLiteral *> literals;
        const auto expr_list = typeid_cast<const ASTExpressionList *>(inner_function->children.front().get());

        if (!expr_list)
        {
            logDebug("expected ExpressionList in tuple, got: " + inner_function->getID());
            return nullptr;
        }

        for (const auto child : expr_list->children)
        {
            if (const auto child_literal = typeid_cast<const ASTLiteral *>(child.get()))
            {
                literals.push_back(child_literal);
            }
            else
            {
                logDebug("non-literal in IN expression, got: " + child->getID());
                return nullptr;
            }
        }

        return analyzeIn(ident, literals, sharding_key_expr);
    }
    else if (function->name == or_function_name)
    {
        const auto expr_list = typeid_cast<const ASTExpressionList *>(function->children.front().get());

        if (!expr_list)
        {
            logDebug("expected ExpressionList in IN, got: " + function->getID());
            return nullptr;
        }

        Disjunctions disjunctions;

        for (const auto child : expr_list->children)
        {
            // we can't ignore expr we can't analyze because it can widden the set of shards
            if (const auto child_function = typeid_cast<const ASTFunction *>(child.get()))
            {
                const auto child_disjunctions = analyzeQuery(child_function, sharding_key_expr);

                if (!child_disjunctions)
                    return nullptr;

                disjunctions.insert(std::end(disjunctions), child_disjunctions->begin(), child_disjunctions->end());
            }
            else
            {
                logDebug("non-function expression in OR, got: " + child->getID());
                return nullptr;
            }
        }

        return std::make_shared<Disjunctions>(disjunctions);
    }
    else if (function->name == and_function_name)
    {
        const auto expr_list = typeid_cast<const ASTExpressionList *>(function->children.front().get());

        if (!expr_list)
        {
            logDebug("expected ExpressionList in AND, got: " + function->getID());
            return nullptr;
        }

        Disjunctions disjunctions;

        for (const auto child : expr_list->children)
        {
            // we can skip everything we can't analyze because it can only narrow the set of shards
            if (const auto child_function = typeid_cast<const ASTFunction *>(child.get()))
            {
                const auto child_disjunctions = analyzeQuery(child_function, sharding_key_expr);

                if (!child_disjunctions)
                    continue;

                disjunctions = pairwiseAnd(disjunctions, *child_disjunctions);
            }
        }

        return std::make_shared<Disjunctions>(disjunctions);
    }
    else
    {
        logDebug("unsupported function: " + function->name);
        return nullptr;
    }
}

/// the same as DistributedBlockOutputStream::createSelector, should it be static?
IColumn::Selector createSelector(const ClusterPtr cluster, const ColumnWithTypeAndName & result)
{
    const auto & slot_to_shard = cluster->getSlotToShard();

#define CREATE_FOR_TYPE(TYPE)                                   \
    if (typeid_cast<const DataType##TYPE *>(result.type.get())) \
        return createBlockSelector<TYPE>(*result.column, slot_to_shard);

    CREATE_FOR_TYPE(UInt8)
    CREATE_FOR_TYPE(UInt16)
    CREATE_FOR_TYPE(UInt32)
    CREATE_FOR_TYPE(UInt64)
    CREATE_FOR_TYPE(Int8)
    CREATE_FOR_TYPE(Int16)
    CREATE_FOR_TYPE(Int32)
    CREATE_FOR_TYPE(Int64)

#undef CREATE_FOR_TYPE

    throw Exception{"Sharding key expression does not evaluate to an integer type", ErrorCodes::TYPE_MISMATCH};
}

/// Returns true if block has all columns required by sharding_key_expr.
bool hasRequiredColumns(const Block & block, ExpressionActionsPtr sharding_key_expr)
{
    for (const auto name : sharding_key_expr->getRequiredColumns())
    {
        bool hasColumn = false;
        for (const auto column_name : block.getNames())
        {
            if (column_name == name)
            {
                hasColumn = true;
                break;
            }
        }

        if (!hasColumn)
            return false;
    }

    return true;
}

}

/** Returns a new cluster with fewer shards if constant folding for sharding_key_expr is possible
 * using constraints from WHERE condition, otherwise, returns nullptr. */
ClusterPtr StorageDistributed::skipUnusedShards(ClusterPtr cluster, const SelectQueryInfo & query_info)
{
    const auto & select = typeid_cast<ASTSelectQuery &>(*query_info.query);

    if (!select.where_expression)
        return nullptr;

    const auto function = typeid_cast<const ASTFunction *>(select.where_expression.get());

    if (!function)
        return nullptr;

    const auto disjunctions = analyzeQuery(function, sharding_key_expr);

    // didn't get definite answer from analysis, about optimization
    if (!disjunctions)
        return nullptr;

    std::set<int> shards;

    for (const auto disjunction : *disjunctions)
    {
        for (const auto conjunction : disjunction.conjunctions)
        {
            Block block(conjunction.columns);

            // check if sharding_key_expr requires column that we don't know anything about
            // if so, we don't have enough information to optimize
            if (!hasRequiredColumns(block, sharding_key_expr))
                return nullptr;

            sharding_key_expr->execute(block);

            if (!block || block.rows() != 1 || !block.has(sharding_key_column_name))
                throw Exception("Logical error: sharding_key_expr should evaluate as 1 row", ErrorCodes::TYPE_MISMATCH);

            const auto result = block.getByName(sharding_key_column_name);
            const auto selector = createSelector(cluster, result);

            shards.insert(selector.begin(), selector.end());
        }
    }

    return cluster->getClusterWithMultipleShards({shards.begin(), shards.end()});
}

}
