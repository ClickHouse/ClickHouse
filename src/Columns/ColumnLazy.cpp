#include <Columns/ColumnLazy.h>

#include <Columns/ColumnsNumber.h>
#include <Columns/IColumnImpl.h>
#include <Core/Field.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

ColumnLazy::ColumnLazy(MutableColumnPtr && part_nums_, MutableColumnPtr && row_nums_)
    : part_nums(std::move(part_nums_)), row_nums(std::move(row_nums_))
{
    if (!part_nums && !row_nums)
        return;

    const ColumnUInt64 * part_num_column = typeid_cast<const ColumnUInt64 *>(part_nums.get());
    const ColumnUInt64 * row_num_column = typeid_cast<const ColumnUInt64 *>(row_nums.get());

    if (!part_num_column)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "'part_nums' column must be a ColumnUInt64, got: {}", part_nums->getName());

    if (!row_num_column)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "'row_nums' column must be a ColumnUInt64, got: {}", row_nums->getName());

    if (part_nums->size() != row_nums->size())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "part_nums size ({}) is inconsistent with row_nums size ({})", part_nums->size(), row_nums->size());
}

ColumnLazy::Ptr ColumnLazy::create(const ColumnPtr & part_nums_, const ColumnPtr & row_nums_)
{
    auto column_lazy = ColumnLazy::create(MutableColumnPtr(), MutableColumnPtr());
    column_lazy->part_nums = part_nums_;
    column_lazy->row_nums = row_nums_;

    return column_lazy;
}

ColumnLazy::Ptr ColumnLazy::create()
{
    MutableColumnPtr part_nums_ = ColumnUInt64::create();
    MutableColumnPtr row_nums_ = ColumnUInt64::create();

    return ColumnLazy::create(std::move(part_nums_), std::move(row_nums_));
}

MutableColumnPtr ColumnLazy::cloneEmpty() const
{
    return ColumnLazy::create()->assumeMutable();
}

MutableColumnPtr ColumnLazy::cloneResized(size_t new_size) const
{
    MutableColumnPtr part_nums_ = part_nums->cloneResized(new_size);
    MutableColumnPtr row_nums_ = row_nums->cloneResized(new_size);
    return ColumnLazy::create(std::move(part_nums_), std::move(row_nums_));
}

Field ColumnLazy::operator[](size_t n) const
{
    Field res;
    get(n, res);
    return res;
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

void ColumnLazy::insertFrom(const IColumn & src_, size_t n)
{
    const ColumnLazy & src = assert_cast<const ColumnLazy &>(src_);

    part_nums->insertFrom(src.getPartNumsColumn(), n);
    row_nums->insertFrom(src.getRowNumsColumn(), n);
}

void ColumnLazy::insertDefault()
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method insertDefault is not supported for {}", getName());
}

void ColumnLazy::popBack(size_t)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method popBack is not supported for {}", getName());
}

StringRef ColumnLazy::serializeValueIntoArena(size_t, Arena &, char const *&, const UInt8 *) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method serializeValueIntoArena is not supported for {}", getName());
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
    const ColumnLazy & src_column_lazy = assert_cast<const ColumnLazy &>(src);

    part_nums->insertRangeFrom(src_column_lazy.getPartNumsColumn(), start, length);
    row_nums->insertRangeFrom(src_column_lazy.getRowNumsColumn(), start, length);
}

ColumnPtr ColumnLazy::filter(const Filter & filt, ssize_t result_size_hint) const
{
    ColumnPtr part_nums_ = part_nums->filter(filt, result_size_hint);
    ColumnPtr row_nums_ = row_nums->filter(filt, result_size_hint);

    return ColumnLazy::create(part_nums_, row_nums_);
}

void ColumnLazy::expand(const Filter &, bool)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method expand is not supported for {}", getName());
}

ColumnPtr ColumnLazy::permute(const Permutation & perm, size_t limit) const
{
    ColumnPtr part_nums_ = part_nums->permute(perm, limit);
    ColumnPtr row_nums_ = row_nums->permute(perm, limit);

    return ColumnLazy::create(part_nums_, row_nums_);
}

ColumnPtr ColumnLazy::index(const IColumn & indexes, size_t limit) const
{
    ColumnPtr part_nums_ = part_nums->index(indexes, limit);
    ColumnPtr row_nums_ = row_nums->index(indexes, limit);

    return ColumnLazy::create(part_nums_, row_nums_);
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
    part_nums->reserve(n);
    row_nums->reserve(n);
}

void ColumnLazy::ensureOwnership()
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method ensureOwnership is not supported for {}", getName());
}

size_t ColumnLazy::byteSize() const
{
    return part_nums->byteSize() + row_nums->byteSize();
}

size_t ColumnLazy::byteSizeAt(size_t) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method byteSizeAt is not supported for {}", getName());
}

size_t ColumnLazy::allocatedBytes() const
{
    return part_nums->allocatedBytes() + row_nums->allocatedBytes();
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

}
