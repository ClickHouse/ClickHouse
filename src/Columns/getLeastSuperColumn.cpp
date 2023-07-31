#include <Columns/getLeastSuperColumn.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnConst.h>
#include <Common/assert_cast.h>
#include <DataTypes/getLeastSupertype.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static bool sameConstants(const IColumn & a, const IColumn & b)
{
    return assert_cast<const ColumnConst &>(a).getField() == assert_cast<const ColumnConst &>(b).getField();
}

ColumnWithTypeAndName getLeastSuperColumn(const std::vector<const ColumnWithTypeAndName *> & columns)
{
    if (columns.empty())
        throw Exception("Logical error: no src columns for supercolumn", ErrorCodes::LOGICAL_ERROR);

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

    result.type = getLeastSupertype(types);

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
