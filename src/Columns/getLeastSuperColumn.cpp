#include <Columns/getLeastSuperColumn.h>
#include <Columns/IColumn.h>
#include <Common/Exception.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnConst.h>
#include <Common/assert_cast.h>
#include <Core/Field.h>
#include <DataTypes/getLeastSupertype.h>

#include <bit>


namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

static bool containsAggregateStateColumn(const IColumn & column)
{
    if (typeid_cast<const ColumnAggregateFunction *>(&column))
        return true;

    bool found = false;
    column.forEachSubcolumn([&](const auto & subcolumn) { found = found || containsAggregateStateColumn(*subcolumn); });
    return found;
}

/// Field::operator== compares Float64 values via FloatCompareHelper, which treats -0.0
/// as equal to 0.0 and any two NaN payloads as equal. A constant UNION output column
/// carries one branch's stored bits for every row, so constness may only be kept when
/// the constants are bit-identical.
static bool sameConstantFields(const Field & lhs, const Field & rhs);

template <typename FieldVectorT>
static bool sameConstantFieldVectors(const FieldVectorT & lhs, const FieldVectorT & rhs)
{
    if (lhs.size() != rhs.size())
        return false;
    for (size_t i = 0; i < lhs.size(); ++i)
        if (!sameConstantFields(lhs[i], rhs[i]))
            return false;
    return true;
}

static bool sameConstantFields(const Field & lhs, const Field & rhs)
{
    if (lhs.getType() != rhs.getType())
        return false;

    switch (lhs.getType())
    {
        case Field::Types::Float64:
            return std::bit_cast<UInt64>(lhs.get<Float64>()) == std::bit_cast<UInt64>(rhs.get<Float64>());
        case Field::Types::Array:
            return sameConstantFieldVectors(lhs.get<Array>(), rhs.get<Array>());
        case Field::Types::Tuple:
            return sameConstantFieldVectors(lhs.get<Tuple>(), rhs.get<Tuple>());
        case Field::Types::Map:
            return sameConstantFieldVectors(lhs.get<Map>(), rhs.get<Map>());
        default:
            return lhs == rhs;
    }
}

static bool sameConstants(const IColumn & a, const IColumn & b)
{
    /// Aggregate-state values cannot be compared as `Field`: the comparison throws when the
    /// aggregate function type names differ, and they may legitimately differ between `UNION`
    /// branches when the functions have the same state representation (e.g. `quantileState`
    /// and `quantilesState(0.9)`). Don't save constness for them.
    if (containsAggregateStateColumn(assert_cast<const ColumnConst &>(a).getDataColumn()))
        return false;

    return sameConstantFields(assert_cast<const ColumnConst &>(a).getField(), assert_cast<const ColumnConst &>(b).getField());
}

ColumnsWithTypeAndName reconcileConstness(
    const ColumnsWithTypeAndName & reference,
    size_t num_siblings,
    const std::function<const ColumnWithTypeAndName *(size_t sibling, size_t position, const String & name)> & lookup,
    bool * materialized)
{
    ColumnsWithTypeAndName common = reference;

    for (size_t col = 0; col < common.size(); ++col)
    {
        if (!common[col].column || !isColumnConst(*common[col].column))
            continue;

        if (containsAggregateStateColumn(assert_cast<const ColumnConst &>(*common[col].column).getDataColumn()))
        {
            common[col].column = common[col].column->convertToFullColumnIfConst();
            if (materialized)
                *materialized = true;
            continue;
        }

        const Field value = assert_cast<const ColumnConst &>(*common[col].column).getField();
        bool keep_const = true;
        for (size_t sibling = 0; sibling < num_siblings; ++sibling)
        {
            const auto * branch = lookup(sibling, col, common[col].name);
            if (!branch || !branch->column || !isColumnConst(*branch->column)
                || !sameConstantFields(assert_cast<const ColumnConst &>(*branch->column).getField(), value))
            {
                keep_const = false;
                break;
            }
        }

        if (!keep_const)
        {
            common[col].column = common[col].column->convertToFullColumnIfConst();
            if (materialized)
                *materialized = true;
        }
    }

    return common;
}

ColumnWithTypeAndName getLeastSuperColumn(const VectorWithMemoryTracking<const ColumnWithTypeAndName *> & columns, bool use_variant_as_common_type)
{
    if (columns.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No src columns for supercolumn");

    ColumnWithTypeAndName result = *columns[0];

    /// Determine common type.

    size_t num_const = 0;
    DataTypes types(columns.size());
    for (size_t i = 0; i < columns.size(); ++i)
    {
        types[i] = columns[i]->type;
        if (isColumnConst(*columns[i]->column))
            ++num_const;
    }

    result.type = use_variant_as_common_type ? getLeastSupertypeOrVariant(types) : getLeastSupertype(types);

    /// Create supertype column saving constness if possible.

    bool save_constness = false;
    if (columns.size() == num_const)
    {
        save_constness = true;
        for (size_t i = 1; i < columns.size(); ++i)
        {
            const ColumnWithTypeAndName & first = *columns[0];
            const ColumnWithTypeAndName & other = *columns[i];

            if (!sameConstants(*first.column, *other.column))
            {
                save_constness = false;
                break;
            }
        }
    }

    if (save_constness)
        result.column = result.type->createColumnConst(0, assert_cast<const ColumnConst &>(*columns[0]->column).getField());
    else
        result.column = result.type->createColumn();

    return result;
}

}
