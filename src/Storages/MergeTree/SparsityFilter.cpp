#include <Storages/MergeTree/SparsityFilter.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/TableNode.h>
#include <Storages/StorageSnapshot.h>
#include <Columns/ColumnNullable.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/getLeastSupertype.h>
#include <Interpreters/convertFieldToType.h>


namespace DB
{

namespace
{

/// Mirrors how the analyzer compares two values of related but not identical
/// types (e.g. `DateTime` column vs `DateTime64` constant): promote both sides
/// to their least common supertype and compare there. `strict = true` rejects
/// lossy conversions, e.g. `toDecimal32('0.001', 3)` truncated to `0.00` under
/// a `Decimal(9, 2)` column. The Nullable wrapper is kept on purpose: for a
/// Nullable column the per-column `num_defaults` counts NULLs, so a literal
/// like `0` does not match the Nullable default (`NULL`) and the predicate
/// falls through here.
bool constantEqualsTypeDefault(const Field & value, const DataTypePtr & value_type, const DataTypePtr & column_type)
{
    auto common_type = tryGetLeastSupertype(DataTypes{value_type, column_type});
    if (!common_type)
        return false;
    Field lhs = tryConvertFieldToType(value, *common_type, value_type.get(), {}, /*strict=*/true);
    if (lhs.isNull())
        return false;
    Field rhs = tryConvertFieldToType(column_type->getDefault(), *common_type, column_type.get(), {}, /*strict=*/true);
    if (rhs.isNull())
        return false;
    return lhs == rhs;
}

/// Range predicates on unsigned integers like `col > 0` match all non default rows.
/// For Nullable(UInt) the predicate evaluates to NULL on NULL rows, which `WHERE`
/// treats as false, so the meaning is "non NULL and > 0" rather than "non default".
/// Reject Nullable to avoid the mismatch.
bool isUnsignedInteger(const DataTypePtr & type)
{
    return !type->isNullable() && WhichDataType(*type).isUInt();
}

/// Same reasoning as `isUnsignedInteger`: `empty(col)` and `notEmpty(col)` on a
/// Nullable String return NULL for NULL rows, which is not the same as "row is
/// default" for the per column stat.
bool isStringLike(const DataTypePtr & type)
{
    return !type->isNullable() && WhichDataType(*type).isString();
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

/// `WHERE n IS NULL` is rewritten by the analyzer to `WHERE n.null`, a UInt8
/// `ColumnNode` for the `.null` subcolumn of `n` (1 for NULL, 0 otherwise). The
/// rewrite uses the simple `NameAndTypePair(name, type)` constructor and does not
/// set `subcolumn_delimiter_position`, so `isSubcolumn()` is false here. Match by
/// name suffix and type, confirm the base column is Nullable in storage, and bail
/// out when a literal top level column named `<base>.null` exists (in that case
/// the analyzer leaves `isNull` alone and any `WHERE n.null` we see refers to the
/// literal column).
std::optional<String>
tryAsNullSubcolumnOf(const QueryTreeNodePtr & node, const QueryTreeNodePtr & expected_table_expression)
{
    static constexpr std::string_view kNullSuffix = ".null";
    const auto * col = node->as<ColumnNode>();
    if (!col)
        return std::nullopt;
    auto source = col->getColumnSourceOrNull();
    if (!source || source.get() != expected_table_expression.get())
        return std::nullopt;
    const auto & nt = col->getColumn();
    if (!WhichDataType(nt.type).isUInt8() || !nt.name.ends_with(kNullSuffix))
        return std::nullopt;
    String base = nt.name.substr(0, nt.name.size() - kNullSuffix.size());
    const auto * table_node = expected_table_expression->as<TableNode>();
    if (!table_node)
        return std::nullopt;
    const auto & snapshot = *table_node->getStorageSnapshot();
    auto base_in_storage = snapshot.tryGetColumn(GetColumnsOptions(GetColumnsOptions::AllPhysical), base);
    if (!base_in_storage || !base_in_storage->type->isNullable())
        return std::nullopt;
    /// A real top level column named exactly `<base>.null` takes precedence over the
    /// synthesised subcolumn.
    if (snapshot.tryGetColumn(GetColumnsOptions(GetColumnsOptions::All), nt.name).has_value())
        return std::nullopt;
    return base;
}

const ConstantNode * tryAsConstantNode(const QueryTreeNodePtr & node)
{
    return node->as<ConstantNode>();
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

    /// The analyzer rewrites `n IS NULL` to a bare `n.null` ColumnNode (not wrapped
    /// in an `isNull` function), so recognise that form before requiring a function
    /// node.
    if (auto base = tryAsNullSubcolumnOf(predicate, table_expression_node))
        return RecognisedSparsityPredicate{*base, SparsityPredicateClass::MatchesDefault};

    const auto * func = predicate->as<FunctionNode>();
    if (!func)
        return std::nullopt;

    const auto & name = func->getFunctionName();
    const auto & args = func->getArguments().getNodes();

    if (args.size() == 1)
    {
        /// `n IS NOT NULL` becomes `not(n.null)` after the analyzer rewrites
        /// `isNotNull`. Recognise it before the generic single argument column ref
        /// path.
        if (name == "not")
        {
            if (auto base = tryAsNullSubcolumnOf(args[0], table_expression_node))
                return RecognisedSparsityPredicate{*base, SparsityPredicateClass::MatchesNonDefault};
            return std::nullopt;
        }

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
    const auto * const_node = col_opt ? tryAsConstantNode(args[1]) : nullptr;
    bool symmetric = (name == "equals" || name == "notEquals");
    if (!col_opt || !const_node)
    {
        if (!symmetric)
            return std::nullopt;
        col_opt = tryAsColumnRef(args[1], table_expression_node);
        const_node = col_opt ? tryAsConstantNode(args[0]) : nullptr;
        if (!col_opt || !const_node)
            return std::nullopt;
    }
    const auto & col = *col_opt;
    const auto & value = const_node->getValue();

    if (value.isNull())
        return std::nullopt;

    /// `Bool` has a two-element value space, so `= true` / `!= true` partition the
    /// column cleanly even though `true` is not the type default.
    bool col_is_bool = isBool(col.type);

    if (name == "equals")
    {
        if (constantEqualsTypeDefault(value, const_node->getResultType(), col.type))
            return RecognisedSparsityPredicate{col.name, SparsityPredicateClass::MatchesDefault};
        if (col_is_bool && isOne(value))
            return RecognisedSparsityPredicate{col.name, SparsityPredicateClass::MatchesNonDefault};
        return std::nullopt;
    }
    if (name == "notEquals")
    {
        if (constantEqualsTypeDefault(value, const_node->getResultType(), col.type))
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
    const auto * orig_const = orig_col ? tryAsConstantNode(args[1]) : nullptr;
    if (!orig_col || !orig_const)
        return std::nullopt;

    if (name == "greater")
    {
        if (isZero(orig_const->getValue()))
            return RecognisedSparsityPredicate{col.name, SparsityPredicateClass::MatchesNonDefault};
        return std::nullopt;
    }
    if (name == "greaterOrEquals")
    {
        if (isOne(orig_const->getValue()))
            return RecognisedSparsityPredicate{col.name, SparsityPredicateClass::MatchesNonDefault};
        return std::nullopt;
    }
    if (name == "less")
    {
        if (isOne(orig_const->getValue()))
            return RecognisedSparsityPredicate{col.name, SparsityPredicateClass::MatchesDefault};
        return std::nullopt;
    }
    if (name == "lessOrEquals")
    {
        if (isZero(orig_const->getValue()))
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
