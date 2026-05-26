#include <Storages/MergeTree/SparsityFilter.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Columns/ColumnNullable.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/convertFieldToType.h>


namespace DB
{

namespace
{

/// `Field::operator==` is type-strict: `Field(UInt64{0})` (how literal `0` parses)
/// doesn't compare equal to `Field(Int64{0})` (Int16's default). Convert through
/// the column's data type so both sides share a `Types::Which` before comparing.
bool constantEqualsTypeDefault(const Field & value, const DataTypePtr & type)
{
    DataTypePtr inner_type = type;
    if (inner_type->isNullable())
        inner_type = removeNullable(inner_type);
    Field converted = convertFieldToType(value, *inner_type);
    if (converted.isNull())
        return false;
    return converted == inner_type->getDefault();
}

bool isUnsignedInteger(const DataTypePtr & type)
{
    DataTypePtr inner = type;
    if (inner->isNullable())
        inner = removeNullable(inner);
    return WhichDataType(inner).isUInt();
}

bool isStringLike(const DataTypePtr & type)
{
    DataTypePtr inner = type;
    if (inner->isNullable())
        inner = removeNullable(inner);
    return WhichDataType(inner).isString();
}

struct ColumnRef
{
    String name;
    DataTypePtr type;
};

std::optional<ColumnRef>
tryAsColumnRef(const QueryTreeNodePtr & node, const QueryTreeNodePtr & expected_table_expression)
{
    const auto * col = node->as<ColumnNode>();
    if (!col)
        return std::nullopt;
    auto source = col->getColumnSourceOrNull();
    if (!source || source.get() != expected_table_expression.get())
        return std::nullopt;
    return ColumnRef{col->getColumnName(), col->getColumnType()};
}

std::optional<Field> tryAsConstantValue(const QueryTreeNodePtr & node)
{
    const auto * c = node->as<ConstantNode>();
    if (!c)
        return std::nullopt;
    return c->getValue();
}

bool isZero(const Field & value)
{
    if (value.isNull())
        return false;
    if (value.getType() == Field::Types::UInt64) return value.safeGet<UInt64>() == 0;
    if (value.getType() == Field::Types::Int64)  return value.safeGet<Int64>()  == 0;
    if (value.getType() == Field::Types::Float64)return value.safeGet<Float64>()== 0.0;
    return false;
}

bool isOne(const Field & value)
{
    if (value.isNull())
        return false;
    if (value.getType() == Field::Types::UInt64) return value.safeGet<UInt64>() == 1;
    if (value.getType() == Field::Types::Int64)  return value.safeGet<Int64>()  == 1;
    if (value.getType() == Field::Types::Float64)return value.safeGet<Float64>()== 1.0;
    return false;
}

}

std::optional<RecognisedSparsityPredicate>
classifySparsityPredicate(const QueryTreeNodePtr & predicate, const QueryTreeNodePtr & table_expression_node)
{
    if (!predicate)
        return std::nullopt;

    const auto * func = predicate->as<FunctionNode>();
    if (!func)
        return std::nullopt;

    const auto & name = func->getFunctionName();
    const auto & args = func->getArguments().getNodes();

    if (args.size() == 1)
    {
        auto col = tryAsColumnRef(args[0], table_expression_node);
        if (!col)
            return std::nullopt;

        if (name == "isNull")
        {
            if (!col->type->isNullable())
                return std::nullopt;
            return RecognisedSparsityPredicate{col->name, SparsityPredicateClass::MatchesDefault};
        }
        if (name == "isNotNull")
        {
            if (!col->type->isNullable())
                return std::nullopt;
            return RecognisedSparsityPredicate{col->name, SparsityPredicateClass::MatchesNonDefault};
        }
        if (name == "empty")
        {
            if (!isStringLike(col->type))
                return std::nullopt;
            return RecognisedSparsityPredicate{col->name, SparsityPredicateClass::MatchesDefault};
        }
        if (name == "notEmpty")
        {
            if (!isStringLike(col->type))
                return std::nullopt;
            return RecognisedSparsityPredicate{col->name, SparsityPredicateClass::MatchesNonDefault};
        }
        return std::nullopt;
    }

    if (args.size() != 2)
        return std::nullopt;

    /// For symmetric operators we also try the (const, col) ordering.
    auto col_opt = tryAsColumnRef(args[0], table_expression_node);
    auto const_opt = col_opt ? tryAsConstantValue(args[1]) : std::nullopt;
    bool symmetric = (name == "equals" || name == "notEquals");
    if (!col_opt || !const_opt.has_value())
    {
        if (!symmetric)
            return std::nullopt;
        col_opt = tryAsColumnRef(args[1], table_expression_node);
        const_opt = col_opt ? tryAsConstantValue(args[0]) : std::nullopt;
        if (!col_opt || !const_opt.has_value())
            return std::nullopt;
    }
    const auto & col = *col_opt;
    const auto & value = *const_opt;

    if (value.isNull())
        return std::nullopt;

    /// `Bool` has a two-element value space, so `= true` / `!= true` partition the
    /// column cleanly even though `true` is not the type default.
    bool col_is_bool = isBool(col.type);

    if (name == "equals")
    {
        if (constantEqualsTypeDefault(value, col.type))
            return RecognisedSparsityPredicate{col.name, SparsityPredicateClass::MatchesDefault};
        if (col_is_bool && isOne(value))
            return RecognisedSparsityPredicate{col.name, SparsityPredicateClass::MatchesNonDefault};
        return std::nullopt;
    }
    if (name == "notEquals")
    {
        if (constantEqualsTypeDefault(value, col.type))
            return RecognisedSparsityPredicate{col.name, SparsityPredicateClass::MatchesNonDefault};
        if (col_is_bool && isOne(value))
            return RecognisedSparsityPredicate{col.name, SparsityPredicateClass::MatchesDefault};
        return std::nullopt;
    }

    /// Asymmetric ops require unsigned integers so that `0` is the only non-positive
    /// value and the column-on-left, constant-on-right ordering is fixed.
    if (!isUnsignedInteger(col.type))
        return std::nullopt;

    auto orig_col = tryAsColumnRef(args[0], table_expression_node);
    auto orig_const = orig_col ? tryAsConstantValue(args[1]) : std::nullopt;
    if (!orig_col || !orig_const.has_value())
        return std::nullopt;

    if (name == "greater")
    {
        if (isZero(*orig_const))
            return RecognisedSparsityPredicate{col.name, SparsityPredicateClass::MatchesNonDefault};
        return std::nullopt;
    }
    if (name == "greaterOrEquals")
    {
        if (isOne(*orig_const))
            return RecognisedSparsityPredicate{col.name, SparsityPredicateClass::MatchesNonDefault};
        return std::nullopt;
    }
    if (name == "less")
    {
        if (isOne(*orig_const))
            return RecognisedSparsityPredicate{col.name, SparsityPredicateClass::MatchesDefault};
        return std::nullopt;
    }
    if (name == "lessOrEquals")
    {
        if (isZero(*orig_const))
            return RecognisedSparsityPredicate{col.name, SparsityPredicateClass::MatchesDefault};
        return std::nullopt;
    }

    return std::nullopt;
}

std::vector<RecognisedSparsityPredicate>
collectSparsityConjuncts(const QueryTreeNodePtr & predicate, const QueryTreeNodePtr & table_expression_node)
{
    std::vector<RecognisedSparsityPredicate> out;
    if (!predicate)
        return out;

    if (const auto * func = predicate->as<FunctionNode>(); func && func->getFunctionName() == "and")
    {
        for (const auto & arg : func->getArguments().getNodes())
        {
            auto child = collectSparsityConjuncts(arg, table_expression_node);
            out.insert(out.end(), child.begin(), child.end());
        }
    }
    else if (auto classified = classifySparsityPredicate(predicate, table_expression_node))
    {
        out.push_back(*classified);
    }

    /// Drop duplicates: the analyzer would otherwise re-run the same `(part, column,
    /// class)` classification once per duplicate, and on a cold query the duplicates
    /// race the `QueryConditionCache` slot rather than waiting on the first call.
    /// We sort+unique at every recursion level so the top-level result is deduped even
    /// when the duplicates appear in sibling subtrees of the AND tree.
    std::sort(out.begin(), out.end(),
        [](const RecognisedSparsityPredicate & a, const RecognisedSparsityPredicate & b)
        {
            return std::tie(a.column_name, a.predicate_class)
                 < std::tie(b.column_name, b.predicate_class);
        });
    out.erase(std::unique(out.begin(), out.end(),
        [](const RecognisedSparsityPredicate & a, const RecognisedSparsityPredicate & b)
        {
            return a.column_name == b.column_name && a.predicate_class == b.predicate_class;
        }), out.end());
    return out;
}

}
