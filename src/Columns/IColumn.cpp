#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnConst.h>
#include <Core/Field.h>


namespace DB
{

String IColumn::dumpStructure() const
{
    WriteBufferFromOwnString res;
    res << getFamilyName() << "(size = " << size();

    ColumnCallback callback = [&](ColumnPtr & subcolumn)
    {
        res << ", " << subcolumn->dumpStructure();
    };

    const_cast<IColumn*>(this)->forEachSubcolumn(callback);

    res << ")";
    return res.str();
}

void IColumn::insertFrom(const IColumn & src, size_t n)
{
    insert(src[n]);
}

ColumnPtr IColumn::createWithOffsets(const Offsets & offsets, size_t total_rows) const
{
    auto res = cloneEmpty();
    res->reserve(total_rows);

    size_t current_offset = 0;
    for (size_t i = 0; i < offsets.size(); ++i)
    {
        size_t offsets_diff = offsets[i] - current_offset;
        current_offset = offsets[i];

        if (offsets_diff > 1)
            res->insertManyFrom(*this, 0, offsets_diff - 1);

        res->insertFrom(*this, i + 1);
    }

    size_t offsets_diff = total_rows - current_offset;
    if(offsets_diff > 1)
        res->insertManyFrom(*this, 0, offsets_diff - 1);

    return res;
}

bool isColumnNullable(const IColumn & column)
{
    return checkColumn<ColumnNullable>(column);
}

bool isColumnConst(const IColumn & column)
{
    return checkColumn<ColumnConst>(column);
}

}
