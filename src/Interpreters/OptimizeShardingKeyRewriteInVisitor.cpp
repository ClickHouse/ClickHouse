#include <Interpreters/OptimizeShardingKeyRewriteInVisitor.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/Utils.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
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

/// The source type of a constant matters for conversions that change the layout of the value while
/// keeping the same `Field` representation - notably `UUID` and `UUID2`, which differ only by the order
/// of the two 64-bit halves. Without the hint `convertFieldToType` leaves such a constant untouched, the
/// sharding expression is then evaluated on the wrong 16 bytes and the shard that owns the row can be
/// pruned. `Nullable`/`LowCardinality` wrappers are stripped because the hint is compared against the
/// destination type of the value itself.
DataTypePtr getSourceTypeHint(const DataTypePtr & type)
{
    if (!type)
        return nullptr;
    return removeNullable(removeLowCardinality(type));
}

/// @param column_value - one of values from IN
/// @param sharding_column_name - name of that column
/// @param from_type_hint - type of the constant `column_value` came from, if known
/// @return true if shard may contain such value (or it is unknown), otherwise false.
bool shardContains(
    Field column_value,
    const std::string & sharding_column_name,
    const OptimizeShardingKeyRewriteInMatcher::Data & data,
    const IDataType * from_type_hint = nullptr)
{
    /// Type of column in storage (used for implicit conversion from i.e. String to Int)
    const DataTypePtr & column_type = data.sharding_key_expr->getSampleBlock().getByName(sharding_column_name).type;
    /// Implicit conversion.
    column_value = convertFieldToType(column_value, *column_type, from_type_hint);

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

    /// Without the analyzer only plain literals are rewritten, and a literal carries no data type: a
    /// `toUUID(...)` call stays an `ASTFunction` and is left alone (the shard is kept, never pruned),
    /// while a string literal is parsed into the destination type by text deserialization, which already
    /// yields the right layout for both `UUID` and `UUID2`. So there is no source type to thread here.
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
        if (!function_node || function_node->getFunctionName() != "in")
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

                /// Per-element source types of the constant tuple: they tell `shardContains` in which layout
                /// the value is encoded, which is what makes `IN (toUUID(...))` over a `UUID2` sharding key
                /// (and the other way around) prune the right shards.
                const auto & element_types = assert_cast<const DataTypeTuple &>(*constant->getResultType()).getElements();

                Tuple new_tuple;
                new_tuple.reserve(tuple.size());
                DataTypes new_element_types;
                new_element_types.reserve(tuple.size());
                const bool have_element_types = element_types.size() == tuple.size();

                for (size_t i = 0; i < tuple.size(); ++i)
                {
                    auto element_type_hint = have_element_types ? getSourceTypeHint(element_types[i]) : nullptr;
                    if (shardContains(tuple[i], name, data, element_type_hint.get()))
                    {
                        new_tuple.push_back(tuple[i]);
                        if (have_element_types)
                            new_element_types.push_back(element_types[i]);
                    }
                }

                if (new_tuple.empty())
                {
                    new_tuple.push_back(tuple.back());
                    if (have_element_types)
                        new_element_types.push_back(element_types.back());
                }

                if (new_tuple.size() == tuple.size())
                    return;

                /// Keep the element types: a value whose type does not survive a literal round trip - such as
                /// `UUID2`, which is formatted with `UUID` semantics unless the constant carries its type - would
                /// otherwise be sent to the shard as a different value.
                if (!have_element_types)
                    arguments[1] = std::make_shared<ConstantNode>(std::move(new_tuple));
                else
                    arguments[1] = std::make_shared<ConstantNode>(
                        Field(std::move(new_tuple)), std::make_shared<DataTypeTuple>(std::move(new_element_types)));
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
