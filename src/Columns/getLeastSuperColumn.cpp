#include <Columns/getLeastSuperColumn.h>
#include <Columns/IColumn.h>
#include <Common/Exception.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnConst.h>
#include <Common/assert_cast.h>
#include <Core/Field.h>
#include <DataTypes/getLeastSupertype.h>


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

static bool sameConstants(const IColumn & a, const IColumn & b)
{
    /// Aggregate-state values cannot be compared as `Field`: the comparison throws when the
    /// aggregate function type names differ, and they may legitimately differ between `UNION`
    /// branches when the functions have the same state representation (e.g. `quantileState`
    /// and `quantilesState(0.9)`). Don't save constness for them.
    if (containsAggregateStateColumn(assert_cast<const ColumnConst &>(a).getDataColumn()))
        return false;

    return assert_cast<const ColumnConst &>(a).getField() == assert_cast<const ColumnConst &>(b).getField();
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
                || assert_cast<const ColumnConst &>(*branch->column).getField() != value)
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
