#include <Columns/ColumnLazy.h>

#include <Columns/ColumnsCommon.h>
#include <Columns/IColumnImpl.h>
#include <Columns/IColumnLazyHelper.h>
#include <Core/Field.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}

ColumnLazy::ColumnLazy(MutableColumns && mutable_columns)
{
    captured_columns.reserve(mutable_columns.size());
    for (auto & column : mutable_columns)
        captured_columns.push_back(std::move(column));
}

ColumnLazy::Ptr ColumnLazy::create(const Columns & columns, ColumnLazyHelperPtr column_lazy_helper)
{
    auto column_lazy = ColumnLazy::create(MutableColumns());
    column_lazy->captured_columns.assign(columns.begin(), columns.end());
    column_lazy->column_lazy_helper = column_lazy_helper;

    return column_lazy;
}

ColumnLazy::Ptr ColumnLazy::create(const CapturedColumns & columns, ColumnLazyHelperPtr column_lazy_helper)
{
    auto column_lazy = ColumnLazy::create(MutableColumns());
    column_lazy->captured_columns = columns;
    column_lazy->column_lazy_helper = column_lazy_helper;

    return column_lazy;
}

ColumnLazy::Ptr ColumnLazy::create(size_t s)
{
    auto column_lazy = ColumnLazy::create(MutableColumns());
    column_lazy->s = s;
    return column_lazy;
}

MutableColumnPtr ColumnLazy::cloneResized(size_t new_size) const
{
    if (!column_lazy_helper)
        return ColumnLazy::create(new_size)->assumeMutable();

    const size_t column_size = captured_columns.size();
    MutableColumns new_columns(column_size);
    for (size_t i = 0; i < column_size; ++i)
        new_columns[i] = captured_columns[i]->cloneResized(new_size);

    auto column_lazy = ColumnLazy::create(std::move(new_columns));
    column_lazy->column_lazy_helper = column_lazy_helper;
    return column_lazy;
}

Field ColumnLazy::operator[](size_t) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method operator[] is not supported for {}", getName());
}

void ColumnLazy::get(size_t, Field &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method get is not supported for {}", getName());
}

bool ColumnLazy::isDefaultAt(size_t) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method isDefaultAt is not supported for {}", getName());
}

StringRef ColumnLazy::getDataAt(size_t) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method getDataAt is not supported for {}", getName());
}

void ColumnLazy::insertData(const char *, size_t)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method insertData is not supported for {}", getName());
}

void ColumnLazy::insert(const Field &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method insert is not supported for {}", getName());
}

bool ColumnLazy::tryInsert(const Field &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method tryInsert is not supported for {}", getName());
}

void ColumnLazy::insertFrom(const IColumn & src_, size_t n)
{
    if (!column_lazy_helper)
    {
        ++s;
        return;
    }

    const size_t column_size = captured_columns.size();
    const ColumnLazy & src = assert_cast<const ColumnLazy &>(src_);
    if (src.captured_columns.size() != column_size)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot insert value of different size into ColumnLazy");

    for (size_t i = 0; i < column_size; ++i)
        captured_columns[i]->insertFrom(*src.captured_columns[i], n);
}

void ColumnLazy::insertDefault()
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method insertDefault is not supported for {}", getName());
}

void ColumnLazy::popBack(size_t)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method popBack is not supported for {}", getName());
}

const char * ColumnLazy::deserializeAndInsertFromArena(const char *)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method deserializeAndInsertFromArena is not supported for {}", getName());
}

const char * ColumnLazy::skipSerializedInArena(const char *) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method skipSerializedInArena is not supported for {}", getName());
}

void ColumnLazy::updateHashWithValue(size_t, SipHash &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method updateHashWithValue is not supported for {}", getName());
}

void ColumnLazy::updateWeakHash32(WeakHash32 &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method updateWeakHash32 is not supported for {}", getName());
}

void ColumnLazy::updateHashFast(SipHash &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method updateHashFast is not supported for {}", getName());
}

void ColumnLazy::insertRangeFrom(const IColumn & src, size_t start, size_t length)
{
    if (!column_lazy_helper)
    {
        s += length;
        return;
    }

    const size_t column_size = captured_columns.size();
    for (size_t i = 0; i < column_size; ++i)
        captured_columns[i]->insertRangeFrom(
            *assert_cast<const ColumnLazy &>(src).captured_columns[i],
            start, length);
}

ColumnPtr ColumnLazy::filter(const Filter & filt, ssize_t result_size_hint) const
{
    if (!column_lazy_helper)
    {
        size_t new_size = countBytesInFilter(filt);
        return ColumnLazy::create(new_size);
    }

    const size_t column_size = captured_columns.size();
    Columns new_columns(column_size);

    for (size_t i = 0; i < column_size; ++i)
        new_columns[i] = captured_columns[i]->filter(filt, result_size_hint);

    return ColumnLazy::create(new_columns, column_lazy_helper);
}

void ColumnLazy::expand(const Filter &, bool)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method expand is not supported for {}", getName());
}

ColumnPtr ColumnLazy::permute(const Permutation & perm, size_t limit) const
{
    if (!column_lazy_helper)
    {
        limit = getLimitForPermutation(size(), perm.size(), limit);
        return ColumnLazy::create(limit);
    }

    const size_t column_size = captured_columns.size();
    Columns new_columns(column_size);

    for (size_t i = 0; i < column_size; ++i)
        new_columns[i] = captured_columns[i]->permute(perm, limit);

    return ColumnLazy::create(new_columns, column_lazy_helper);
}

ColumnPtr ColumnLazy::index(const IColumn & indexes, size_t limit) const
{
    if (!column_lazy_helper)
    {
        if (limit == 0)
            limit = indexes.size();
        return ColumnLazy::create(limit);
    }

    const size_t column_size = captured_columns.size();
    Columns new_columns(column_size);

    for (size_t i = 0; i < column_size; ++i)
        new_columns[i] = captured_columns[i]->index(indexes, limit);

    return ColumnLazy::create(new_columns, column_lazy_helper);
}

ColumnPtr ColumnLazy::replicate(const Offsets &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method replicate is not supported for {}", getName());
}

MutableColumns ColumnLazy::scatter(ColumnIndex, const Selector &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method scatter is not supported for {}", getName());
}

int ColumnLazy::compareAt(size_t, size_t, const IColumn &, int) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method compareAt is not supported for {}", getName());
}

void ColumnLazy::compareColumn(const IColumn &, size_t,
                               PaddedPODArray<UInt64> *, PaddedPODArray<Int8> &,
                               int, int) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method compareColumn is not supported for {}", getName());
}

int ColumnLazy::compareAtWithCollation(size_t, size_t, const IColumn &, int, const Collator &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method compareAtWithCollation is not supported for {}", getName());
}

bool ColumnLazy::hasEqualValues() const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method hasEqualValues is not supported for {}", getName());
}

void ColumnLazy::getPermutation(IColumn::PermutationSortDirection, IColumn::PermutationSortStability,
                                size_t, int, IColumn::Permutation &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method getPermutation is not supported for {}", getName());
}

void ColumnLazy::updatePermutation(IColumn::PermutationSortDirection, IColumn::PermutationSortStability,
                                   size_t, int, IColumn::Permutation &, EqualRanges &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method updatePermutation is not supported for {}", getName());
}

void ColumnLazy::getPermutationWithCollation(const Collator &, IColumn::PermutationSortDirection, IColumn::PermutationSortStability, size_t, int, Permutation &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method getPermutationWithCollation is not supported for {}", getName());
}

void ColumnLazy::updatePermutationWithCollation(const Collator &, IColumn::PermutationSortDirection, IColumn::PermutationSortStability, size_t, int, Permutation &, EqualRanges &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method updatePermutationWithCollation is not supported for {}", getName());
}

void ColumnLazy::gather(ColumnGathererStream &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method gather is not supported for {}", getName());
}

void ColumnLazy::reserve(size_t n)
{
    if (!column_lazy_helper)
        return;

    const size_t column_size = captured_columns.size();
    for (size_t i = 0; i < column_size; ++i)
        captured_columns[i]->reserve(n);
}

void ColumnLazy::ensureOwnership()
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method ensureOwnership is not supported for {}", getName());
}

size_t ColumnLazy::byteSize() const
{
    size_t res = 0;
    for (const auto & column : captured_columns)
        res += column->byteSize();
    return res;
}

size_t ColumnLazy::byteSizeAt(size_t) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method byteSizeAt is not supported for {}", getName());
}

size_t ColumnLazy::allocatedBytes() const
{
    size_t res = 0;
    for (const auto & column : captured_columns)
        res += column->allocatedBytes();
    return res;
}

void ColumnLazy::protect()
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method protect is not supported for {}", getName());
}

void ColumnLazy::getExtremes(Field &, Field &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method getExtremes is not supported for {}", getName());
}

void ColumnLazy::forEachSubcolumnRecursively(RecursiveMutableColumnCallback)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method forEachSubcolumnRecursively is not supported for {}", getName());
}

bool ColumnLazy::structureEquals(const IColumn &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method structureEquals is not supported for {}", getName());
}

bool ColumnLazy::isCollationSupported() const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method isCollationSupported is not supported for {}", getName());
}


ColumnPtr ColumnLazy::compress() const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method compress is not supported for {}", getName());
}

double ColumnLazy::getRatioOfDefaultRows(double) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method getRatioOfDefaultRows is not supported for {}", getName());
}

UInt64 ColumnLazy::getNumberOfDefaultRows() const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method getNumberOfDefaultRows is not supported for {}", getName());
}

void ColumnLazy::getIndicesOfNonDefaultRows(Offsets &, size_t, size_t) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method getIndicesOfNonDefaultRows is not supported for {}", getName());
}

void ColumnLazy::finalize()
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method finalize is not supported for {}", getName());
}

bool ColumnLazy::isFinalized() const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method isFinalized is not supported for {}", getName());
}

void ColumnLazy::transform(ColumnsWithTypeAndName & res_columns) const
{
    if (column_lazy_helper)
        column_lazy_helper->transformLazyColumns(*this, res_columns);
}

}
