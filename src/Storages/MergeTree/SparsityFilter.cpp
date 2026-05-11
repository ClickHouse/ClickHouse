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

/// `col != default(col)` requires comparing the constant against the type's default,
/// but Field's operator== is type-strict: a literal `0` parses as Field(UInt64{0})
/// while e.g. Int16's default is Field(Int64{0}), so a naive `value == default_value`
/// returns false even though both numerically represent zero. Convert the constant
/// into the column's data type first; if the conversion succeeds losslessly, compare
/// the resulting Field against the type's default (which is now guaranteed to share
/// the same `Types::Which`).
bool constantEqualsTypeDefault(const Field & value, const DataTypePtr & type)
{
    /// Nullable is handled by the `IS NULL` branch -- here we want to compare the
    /// constant against the *non-null* default of the underlying type.
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
    /// `WhichDataType::isUInt` covers UInt8..UInt256.
    return WhichDataType(inner).isUInt();
}

bool isStringLike(const DataTypePtr & type)
{
    DataTypePtr inner = type;
    if (inner->isNullable())
        inner = removeNullable(inner);
    return WhichDataType(inner).isString();
}

/// If `node` is a ColumnNode whose source is `expected_table_expression`, returns its
/// (name, type). Otherwise nullopt.
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

/// `0` represented as any numeric Field.
bool isZero(const Field & value)
{
    if (value.isNull())
        return false;
    /// A Field's numeric types (UInt64, Int64, Float64) all compare to 0 the way you'd want.
    /// Field == Field handles cross-type integer comparisons correctly.
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

    /// Unary forms: isNull(col) / isNotNull(col) / empty(col) / notEmpty(col).
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

    /// Try (col, const). For symmetric operators (=, !=) also try (const, col).
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

    /// A NULL on a non-Nullable column doesn't match the type's default; skip.
    if (value.isNull())
        return std::nullopt;

    /// Bool's value space is {false, true} = {0, 1}, so `= true` and `!= true` cleanly
    /// partition the column even though they don't match the type's default. We treat
    /// `1` as the only "non-default" value for Bool here; other unsigned types fall
    /// through to the asymmetric comparisons below.
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

    /// Asymmetric integer comparisons: only safe for UNSIGNED columns where 0 is the only
    /// non-positive value, so `col > 0`, `col >= 1`, `col < 1`, `col <= 0` align cleanly
    /// with the default partition. For these the constant must be on the right and the
    /// column on the left -- we already returned for symmetric ops above, so re-check
    /// the (col, const) ordering here.
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

    /// Recursive walk: top-level `AND(a, b, c)` flattens (and recurses for nested ANDs).
    /// Non-AND nodes are classified directly via `classifySparsityPredicate`.
    if (const auto * func = predicate->as<FunctionNode>(); func && func->getFunctionName() == "and")
    {
        for (const auto & arg : func->getArguments().getNodes())
        {
            auto child = collectSparsityConjuncts(arg, table_expression_node);
            out.insert(out.end(), child.begin(), child.end());
        }
        return out;
    }

    if (auto classified = classifySparsityPredicate(predicate, table_expression_node))
        out.push_back(*classified);
    return out;
}

}
