#pragma once

#include <Common/PODArray.h>
#include <Common/memcmpSmall.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <Columns/IColumn.h>
#include <Columns/IColumnImpl.h>
#include <Columns/ColumnFixedSizeHelper.h>
#include <Core/Field.h>


namespace DB
{

/** A column of values of "fixed-length string" type.
  * If you insert a smaller string, it will be padded with zero bytes.
  */
class ColumnFixedString final : public COWHelper<IColumnHelper<ColumnFixedString, ColumnFixedSizeHelper>, ColumnFixedString>
{
public:
    friend class COWHelper<IColumnHelper<ColumnFixedString, ColumnFixedSizeHelper>, ColumnFixedString>;

    using Chars = PaddedPODArray<UInt8>;

private:
    /// Bytes of rows, laid in succession. The strings are stored without a trailing zero byte.
    /** NOTE It is required that the offset and type of chars in the object be the same as that of `data in ColumnUInt8`.
      * Used in `packFixed` function (AggregationCommon.h)
      */
    Chars chars;
    /// The size of the rows.
    const size_t n;

    struct ComparatorBase;

    using ComparatorAscendingUnstable = ComparatorAscendingUnstableImpl<ComparatorBase>;
    using ComparatorAscendingStable = ComparatorAscendingStableImpl<ComparatorBase>;
    using ComparatorDescendingUnstable = ComparatorDescendingUnstableImpl<ComparatorBase>;
    using ComparatorDescendingStable = ComparatorDescendingStableImpl<ComparatorBase>;
    using ComparatorEqual = ComparatorEqualImpl<ComparatorBase>;

    /** Create an empty column of strings of fixed-length `n` */
    explicit ColumnFixedString(size_t n_) : n(n_) {}

    ColumnFixedString(const ColumnFixedString & src) : chars(src.chars.begin(), src.chars.end()), n(src.n) {} /// NOLINT

public:
    std::string getName() const override { return "FixedString(" + std::to_string(n) + ")"; }
    const char * getFamilyName() const override { return "FixedString"; }
    TypeIndex getDataType() const override { return TypeIndex::FixedString; }

    MutableColumnPtr cloneResized(size_t size) const override;

    size_t size() const override
    {
        return chars.size() / n;
    }

    size_t byteSize() const override
    {
        return chars.size() + sizeof(n);
    }

    size_t byteSizeAt(size_t) const override
    {
        return n;
    }

    size_t allocatedBytes() const override
    {
        return chars.allocated_bytes() + sizeof(n);
    }

    void protect() override
    {
        chars.protect();
    }

    Field operator[](size_t index) const override
    {
        return Field{&chars[n * index], n};
    }

    void get(size_t index, Field & res) const override
    {
        res = std::string_view{reinterpret_cast<const char *>(&chars[n * index]), n};
    }

    StringRef getDataAt(size_t index) const override
    {
        return StringRef(&chars[n * index], n);
    }

    bool isDefaultAt(size_t index) const override;

    void insert(const Field & x) override;

    bool tryInsert(const Field & x) override;

#if !defined(DEBUG_OR_SANITIZER_BUILD)
    void insertFrom(const IColumn & src_, size_t index) override;
#else
    void doInsertFrom(const IColumn & src_, size_t index) override;
#endif

#if !defined(DEBUG_OR_SANITIZER_BUILD)
    void insertManyFrom(const IColumn & src, size_t position, size_t length) override;
#else
    void doInsertManyFrom(const IColumn & src, size_t position, size_t length) override;
#endif

    void insertData(const char * pos, size_t length) override;

    void insertDefault() override
    {
        chars.resize_fill(chars.size() + n);
    }

    void insertManyDefaults(size_t length) override
    {
        chars.resize_fill(chars.size() + n * length);
    }

    void popBack(size_t elems) override
    {
        chars.resize_assume_reserved(chars.size() - n * elems);
    }

    const char * deserializeAndInsertFromArena(const char * pos) override;

    const char * skipSerializedInArena(const char * pos) const override;

    void updateHashWithValue(size_t index, SipHash & hash) const override;

    WeakHash32 getWeakHash32() const override;

    void updateHashFast(SipHash & hash) const override;

#if !defined(DEBUG_OR_SANITIZER_BUILD)
    int compareAt(size_t p1, size_t p2, const IColumn & rhs_, int /*nan_direction_hint*/) const override
#else
    int doCompareAt(size_t p1, size_t p2, const IColumn & rhs_, int /*nan_direction_hint*/) const override
#endif
    {
        const ColumnFixedString & rhs = assert_cast<const ColumnFixedString &>(rhs_);
        chassert(this->n == rhs.n);
        return memcmpSmallAllowOverflow15(chars.data() + p1 * n, rhs.chars.data() + p2 * n, n);
    }

    void getPermutation(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                    size_t limit, int nan_direction_hint, Permutation & res) const override;

    void updatePermutation(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                    size_t limit, int nan_direction_hint, Permutation & res, EqualRanges & equal_ranges) const override;

    size_t estimateCardinalityInPermutedRange(const Permutation & permutation, const EqualRange & equal_range) const override;

#if !defined(DEBUG_OR_SANITIZER_BUILD)
    void insertRangeFrom(const IColumn & src, size_t start, size_t length) override;
#else
    void doInsertRangeFrom(const IColumn & src, size_t start, size_t length) override;
#endif

    ColumnPtr filter(const IColumn::Filter & filt, ssize_t result_size_hint) const override;

    void expand(const IColumn::Filter & mask, bool inverted) override;

    ColumnPtr permute(const Permutation & perm, size_t limit) const override;

    ColumnPtr index(const IColumn & indexes, size_t limit) const override;

    template <typename Type>
    ColumnPtr indexImpl(const PaddedPODArray<Type> & indexes, size_t limit) const;

    ColumnPtr replicate(const Offsets & offsets) const override;

    ColumnPtr compress() const override;

    void reserve(size_t size) override
    {
        chars.reserve_exact(n * size);
    }

    size_t capacity() const override
    {
        return chars.capacity() / n;
    }

    void shrinkToFit() override
    {
        chars.shrink_to_fit();
    }

    void resize(size_t size)
    {
        chars.resize(n * size);
    }

    void getExtremes(Field & min, Field & max) const override;

    bool structureEquals(const IColumn & rhs) const override
    {
        if (const auto * rhs_concrete = typeid_cast<const ColumnFixedString *>(&rhs))
            return n == rhs_concrete->n;
        return false;
    }

    bool canBeInsideNullable() const override { return true; }

    bool isFixedAndContiguous() const override { return true; }
    size_t sizeOfValueIfFixed() const override { return n; }
    std::string_view getRawData() const override { return {reinterpret_cast<const char *>(chars.data()), chars.size()}; }

    /// Specialized part of interface, not from IColumn.
    void insertString(const String & string) { insertData(string.c_str(), string.size()); }
    Chars & getChars() { return chars; }
    const Chars & getChars() const { return chars; }

    size_t getN() const { return n; }
};

}
