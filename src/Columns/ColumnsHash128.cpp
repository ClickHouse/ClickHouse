#include <Columns/ColumnFixedSizeHelper.h>
#include <Columns/ColumnsHash128.h>
#include <Common/ColumnsHashing/HashMethod.h>
#include <Common/SipHashStrings.h>

#include <bit>


namespace DB::ColumnsHashing
{

/// The `hash128` of a single `ColumnVector`, `ColumnDecimal` or `ColumnFixedString` hashes the bytes of the value in the
/// column.
bool canHash128InPlace(const ColumnRawPtrs & key_columns)
{
    if constexpr (std::endian::native != std::endian::little)
        return false;
    else
        return sipHashManyRowsIsVectorized() && key_columns.size() == 1 && dynamic_cast<const ColumnFixedSizeHelper *>(key_columns[0]);
}

void hash128Range(const ColumnRawPtrs & key_columns, size_t begin, size_t end, UInt128 * hashes)
{
    if (!canHash128InPlace(key_columns))
    {
        for (size_t row = begin; row < end; ++row)
            hashes[row - begin] = hash128(row, key_columns.size(), key_columns);
        return;
    }

    if (end > begin)
    {
        const size_t value_size = key_columns[0]->sizeOfValueIfFixed();
        sipHash128PaddedFixedStrings(key_columns[0]->getRawData().data() + begin * value_size, value_size, end - begin, hashes);
    }
}

Hash128InOrder::Hash128InOrder(ColumnRawPtrs key_columns_, size_t begin, size_t end_)
    : key_columns(std::move(key_columns_))
    , end(end_)
    , ready_begin(begin)
{
    if (canHash128InPlace(key_columns))
        hashes = std::make_unique<UInt128[]>(chunk_rows);
}

UInt128 Hash128InOrder::hashNext(size_t row)
{
    if (!hashes || row < ready_begin + ready_size || row >= end)
        return hash128(row, key_columns.size(), key_columns);
    ready_begin = row;
    ready_size = std::min(chunk_rows, end - row);
    hash128Range(key_columns, row, row + ready_size, hashes.get());
    return hashes[0];
}

}
