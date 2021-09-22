#include <Interpreters/ExpressionActions.h>
#include <Interpreters/convertFieldToType.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <DataTypes/FieldToDataType.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/OptimizeShardingKeyRewriteInVisitor.h>

namespace
{

using namespace DB;

Field executeFunctionOnField(
    const Field & field, const std::string & name,
    const ExpressionActionsPtr & sharding_expr,
    const std::string & sharding_key_column_name)
{
    DataTypePtr type = applyVisitor(FieldToDataType{}, field);

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

/// @param sharding_column_value - one of values from IN
/// @param sharding_column_name - name of that column
/// @param sharding_expr - expression of sharding_key for the Distributed() table
/// @param sharding_key_column_name - name of the column for sharding_expr
/// @param shard_info - info for the current shard (to compare shard_num with calculated)
/// @param slots - weight -> shard mapping
/// @return true if shard may contain such value (or it is unknown), otherwise false.
bool shardContains(
    const Field & sharding_column_value,
    const std::string & sharding_column_name,
    const ExpressionActionsPtr & sharding_expr,
    const std::string & sharding_key_column_name,
    const Cluster::ShardInfo & shard_info,
    const Cluster::SlotToShard & slots)
{
    /// NULL is not allowed in sharding key,
    /// so it should be safe to assume that shard cannot contain it.
    if (sharding_column_value.isNull())
        return false;

    Field sharding_value = executeFunctionOnField(sharding_column_value, sharding_column_name, sharding_expr, sharding_key_column_name);
    /// The value from IN can be non-numeric,
    /// but in this case it should be convertible to numeric type, let's try.
    sharding_value = convertFieldToType(sharding_value, DataTypeUInt64());
    /// In case of conversion is not possible (NULL), shard cannot contain the value anyway.
    if (sharding_value.isNull())
        return false;

    UInt64 value = sharding_value.get<UInt64>();
    const auto shard_num = slots[value % slots.size()] + 1;
    return shard_info.shard_num == shard_num;
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

    const auto & sharding_expr = data.sharding_key_expr;
    const auto & sharding_key_column_name = data.sharding_key_column_name;

    if (!sharding_expr->getRequiredColumnsWithTypes().contains(identifier->name()))
        return;

    /// NOTE: that we should not take care about empty tuple,
    /// since after optimize_skip_unused_shards,
    /// at least one element should match each shard.
    if (auto * tuple_func = right->as<ASTFunction>(); tuple_func && tuple_func->name == "tuple")
    {
        auto * tuple_elements = tuple_func->children.front()->as<ASTExpressionList>();
        std::erase_if(tuple_elements->children, [&](auto & child)
        {
            auto * literal = child->template as<ASTLiteral>();
            return literal && !shardContains(literal->value, identifier->name(), sharding_expr, sharding_key_column_name, data.shard_info, data.slots);
        });
    }
    else if (auto * tuple_literal = right->as<ASTLiteral>();
        tuple_literal && tuple_literal->value.getType() == Field::Types::Tuple)
    {
        auto & tuple = tuple_literal->value.get<Tuple &>();
        std::erase_if(tuple, [&](auto & child)
        {
            return !shardContains(child, identifier->name(), sharding_expr, sharding_key_column_name, data.shard_info, data.slots);
        });
    }
}

}
