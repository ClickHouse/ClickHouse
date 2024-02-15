#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnConst.h>
#include <Core/Field.h>
#include <DataTypes/Serializations/SerializationInfo.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

String IColumn::dumpStructure() const
{
    WriteBufferFromOwnString res;
    res << getFamilyName() << "(size = " << size();

    forEachSubcolumn([&](const auto & subcolumn)
    {
        res << ", " << subcolumn->dumpStructure();
    });

    res << ")";
    return res.str();
}

void IColumn::insertFrom(const IColumn & src, size_t n)
{
    insert(src[n]);
}

ColumnPtr IColumn::createWithOffsets(const Offsets & offsets, const ColumnConst & column_with_default_value, size_t total_rows, size_t shift) const
{
    if (offsets.size() + shift != size())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Incompatible sizes of offsets ({}), shift ({}) and size of column {}", offsets.size(), shift, size());

    auto res = cloneEmpty();
    res->reserve(total_rows);

    ssize_t current_offset = -1;
    for (size_t i = 0; i < offsets.size(); ++i)
    {
        ssize_t offsets_diff = static_cast<ssize_t>(offsets[i]) - current_offset;
        current_offset = offsets[i];

        if (offsets_diff > 1)
            res->insertManyFrom(column_with_default_value.getDataColumn(), 0, offsets_diff - 1);

        res->insertFrom(*this, i + shift);
    }

    ssize_t offsets_diff = static_cast<ssize_t>(total_rows) - current_offset;
    if (offsets_diff > 1)
        res->insertManyFrom(column_with_default_value.getDataColumn(), 0, offsets_diff - 1);

    return res;
}

void IColumn::forEachSubcolumn(ColumnCallback callback) const
{
    const_cast<IColumn*>(this)->forEachSubcolumn([&callback](WrappedPtr & subcolumn)
    {
        callback(std::as_const(subcolumn));
    });
}

void IColumn::forEachSubcolumnRecursively(RecursiveColumnCallback callback) const
{
    const_cast<IColumn*>(this)->forEachSubcolumnRecursively([&callback](IColumn & subcolumn)
    {
        callback(std::as_const(subcolumn));
    });
}

bool isColumnNullable(const IColumn & column)
{
    return checkColumn<ColumnNullable>(column);
}

bool isColumnNullableOrLowCardinalityNullable(const IColumn & column)
{
    return isColumnNullable(column) || isColumnLowCardinalityNullable(column);
}

bool isColumnConst(const IColumn & column)
{
    return checkColumn<ColumnConst>(column);
}

}
