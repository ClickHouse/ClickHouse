#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnConst.h>
#include <Common/RadixSort.h>


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

bool isColumnNullable(const IColumn & column)
{
    return checkColumn<ColumnNullable>(column);
}

bool isColumnConst(const IColumn & column)
{
    return checkColumn<ColumnConst>(column);
}


namespace
{
    struct RadixSortTraitsBadHash : RadixSortNumTraits<UInt32>
    {
        using Element = IColumn::BadHash;
        static UInt32 & extractKey(Element & elem) { return elem.first; }
    };
}

void IColumn::sortBadHashes(IColumn::BadHashes & hashes)
{
    RadixSort<RadixSortTraitsBadHash>::executeLSD(hashes.data(), hashes.size());
}

}
