#pragma once

#include <cstring>
#include <cassert>

#include <Columns/IColumn.h>
#include <Columns/IColumnImpl.h>
#include <Common/PODArray.h>
#include <Common/SipHash.h>
#include <Common/memcpySmall.h>
#include <Common/memcmpSmall.h>
#include <Common/assert_cast.h>
#include <Core/Field.h>


class Collator;


namespace DB
{

/** Column for String values.
  */
class ColumnString final : public COWHelper<IColumn, ColumnString>
{
public:
    using Char = UInt8;
    using Chars = PaddedPODArray<UInt8>;

private:
    friend class COWHelper<IColumn, ColumnString>;

    /// Maps i'th position to offset to i+1'th element. Last offset maps to the end of all chars (is the size of all chars).
    Offsets offsets;

    /// Bytes of strings, placed contiguously.
    /// For convenience, every string ends with terminating zero byte. Note that strings could contain zero bytes in the middle.
    Chars chars;

    size_t ALWAYS_INLINE offsetAt(ssize_t i) const { return offsets[i - 1]; }

    /// Size of i-th element, including terminating zero.
    size_t ALWAYS_INLINE sizeAt(ssize_t i) const { return offsets[i] - offsets[i - 1]; }

    struct ComparatorBase;

    using ComparatorAscendingUnstable = ComparatorAscendingUnstableImpl<ComparatorBase>;
    using ComparatorAscendingStable = ComparatorAscendingStableImpl<ComparatorBase>;
    using ComparatorDescendingUnstable = ComparatorDescendingUnstableImpl<ComparatorBase>;
    using ComparatorDescendingStable = ComparatorDescendingStableImpl<ComparatorBase>;
    using ComparatorEqual = ComparatorEqualImpl<ComparatorBase>;

    struct ComparatorCollationBase;

    using ComparatorCollationAscendingUnstable = ComparatorAscendingUnstableImpl<ComparatorCollationBase>;
    using ComparatorCollationAscendingStable = ComparatorAscendingStableImpl<ComparatorCollationBase>;
    using ComparatorCollationDescendingUnstable = ComparatorDescendingUnstableImpl<ComparatorCollationBase>;
    using ComparatorCollationDescendingStable = ComparatorDescendingStableImpl<ComparatorCollationBase>;
    using ComparatorCollationEqual = ComparatorEqualImpl<ComparatorCollationBase>;

    ColumnString() = default;
    ColumnString(const ColumnString & src);

public:
    const char * getFamilyName() const override { return "String"; }
    TypeIndex getDataType() const override { return TypeIndex::String; }

    size_t size() const override
    {
        return offsets.size();
    }

    size_t byteSize() const override
    {
        return chars.size() + offsets.size() * sizeof(offsets[0]);
    }

    size_t byteSizeAt(size_t n) const override
    {
        assert(n < size());
        return sizeAt(n) + sizeof(offsets[0]);
    }

    size_t allocatedBytes() const override
    {
        return chars.allocated_bytes() + offsets.allocated_bytes();
    }

    void protect() override;

    MutableColumnPtr cloneResized(size_t to_size) const override;

    Field operator[](size_t n) const override
    {
        assert(n < size());
        return Field(&chars[offsetAt(n)], sizeAt(n) - 1);
    }

    void get(size_t n, Field & res) const override
    {
        assert(n < size());
        res = std::string_view{reinterpret_cast<const char *>(&chars[offsetAt(n)]), sizeAt(n) - 1};
    }

    StringRef getDataAt(size_t n) const override
    {
        assert(n < size());
        return StringRef(&chars[offsetAt(n)], sizeAt(n) - 1);
    }

    StringRef getDataAtWithTerminatingZero(size_t n) const override
    {
        assert(n < size());
        return StringRef(&chars[offsetAt(n)], sizeAt(n));
    }

    bool isDefaultAt(size_t n) const override
    {
        assert(n < size());
        return sizeAt(n) == 1;
    }

/// Suppress gcc 7.3.1 warning: '*((void*)&<anonymous> +8)' may be used uninitialized in this function
#if !defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif

    void insert(const Field & x) override
    {
        const String & s = DB::get<const String &>(x);
        const size_t old_size = chars.size();
        const size_t size_to_append = s.size() + 1;
        const size_t new_size = old_size + size_to_append;

        chars.resize(new_size);
        memcpy(chars.data() + old_size, s.c_str(), size_to_append);
        offsets.push_back(new_size);
    }

#if !defined(__clang__)
#pragma GCC diagnostic pop
#endif

    void insertFrom(const IColumn & src_, size_t n) override
    {
        const ColumnString & src = assert_cast<const ColumnString &>(src_);
        const size_t size_to_append = src.offsets[n] - src.offsets[n - 1];  /// -1th index is Ok, see PaddedPODArray.

        if (size_to_append == 1)
        {
            /// shortcut for empty string
            chars.push_back(0);
            offsets.push_back(chars.size());
        }
        else
        {
            const size_t old_size = chars.size();
            const size_t offset = src.offsets[n - 1];
            const size_t new_size = old_size + size_to_append;

            chars.resize(new_size);
            memcpySmallAllowReadWriteOverflow15(chars.data() + old_size, &src.chars[offset], size_to_append);
            offsets.push_back(new_size);
        }
    }

    void insertData(const char * pos, size_t length) override
    {
        const size_t old_size = chars.size();
        const size_t new_size = old_size + length + 1;

        chars.resize(new_size);
        if (length)
            memcpy(chars.data() + old_size, pos, length);
        chars[old_size + length] = 0;
        offsets.push_back(new_size);
    }

    /// Like getData, but inserting data should be zero-ending (i.e. length is 1 byte greater than real string size).
    void insertDataWithTerminatingZero(const char * pos, size_t length)
    {
        const size_t old_size = chars.size();
        const size_t new_size = old_size + length;

        chars.resize(new_size);
        memcpy(chars.data() + old_size, pos, length);
        offsets.push_back(new_size);
    }

    void popBack(size_t n) override
    {
        size_t nested_n = offsets.back() - offsetAt(offsets.size() - n);
        chars.resize(chars.size() - nested_n);
        offsets.resize_assume_reserved(offsets.size() - n);
    }

    StringRef serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const override;

    const char * deserializeAndInsertFromArena(const char * pos) override;

    const char * skipSerializedInArena(const char * pos) const override;

    void updateHashWithValue(size_t n, SipHash & hash) const override
    {
        size_t string_size = sizeAt(n);
        size_t offset = offsetAt(n);

        hash.update(reinterpret_cast<const char *>(&string_size), sizeof(string_size));
        hash.update(reinterpret_cast<const char *>(&chars[offset]), string_size);
    }

    void updateWeakHash32(WeakHash32 & hash) const override;

    void updateHashFast(SipHash & hash) const override
    {
        hash.update(reinterpret_cast<const char *>(offsets.data()), size() * sizeof(offsets[0]));
        hash.update(reinterpret_cast<const char *>(chars.data()), size() * sizeof(chars[0]));
    }

    void insertRangeFrom(const IColumn & src, size_t start, size_t length) override;

    ColumnPtr filter(const Filter & filt, ssize_t result_size_hint) const override;

    void expand(const Filter & mask, bool inverted) override;

    ColumnPtr permute(const Permutation & perm, size_t limit) const override;

    ColumnPtr index(const IColumn & indexes, size_t limit) const override;

    template <typename Type>
    ColumnPtr indexImpl(const PaddedPODArray<Type> & indexes, size_t limit) const;

    void insertDefault() override
    {
        chars.push_back(0);
        offsets.push_back(offsets.back() + 1);
    }

    virtual void insertManyDefaults(size_t length) override
    {
        chars.resize_fill(chars.size() + length);
        for (size_t i = 0; i < length; ++i)
            offsets.push_back(offsets.back() + 1);
    }

    int compareAt(size_t n, size_t m, const IColumn & rhs_, int /*nan_direction_hint*/) const override
    {
        const ColumnString & rhs = assert_cast<const ColumnString &>(rhs_);
        return memcmpSmallAllowOverflow15(chars.data() + offsetAt(n), sizeAt(n) - 1, rhs.chars.data() + rhs.offsetAt(m), rhs.sizeAt(m) - 1);
    }

    void compareColumn(const IColumn & rhs, size_t rhs_row_num,
                       PaddedPODArray<UInt64> * row_indexes, PaddedPODArray<Int8> & compare_results,
                       int direction, int nan_direction_hint) const override;

    bool hasEqualValues() const override;

    /// Variant of compareAt for string comparison with respect of collation.
    int compareAtWithCollation(size_t n, size_t m, const IColumn & rhs_, int, const Collator & collator) const override;

    void getPermutation(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                    size_t limit, int nan_direction_hint, Permutation & res) const override;

    void updatePermutation(IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                    size_t limit, int, Permutation & res, EqualRanges & equal_ranges) const override;

    /// Sorting with respect of collation.
    void getPermutationWithCollation(const Collator & collator, IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                    size_t limit, int, Permutation & res) const override;

    void updatePermutationWithCollation(const Collator & collator, IColumn::PermutationSortDirection direction, IColumn::PermutationSortStability stability,
                    size_t limit, int, Permutation & res, EqualRanges & equal_ranges) const override;

    ColumnPtr replicate(const Offsets & replicate_offsets) const override;

    MutableColumns scatter(ColumnIndex num_columns, const Selector & selector) const override
    {
        return scatterImpl<ColumnString>(num_columns, selector);
    }

    void gather(ColumnGathererStream & gatherer_stream) override;

    ColumnPtr compress() const override;

    void reserve(size_t n) override;

    void getExtremes(Field & min, Field & max) const override;


    bool canBeInsideNullable() const override { return true; }

    bool structureEquals(const IColumn & rhs) const override
    {
        return typeid(rhs) == typeid(ColumnString);
    }

    double getRatioOfDefaultRows(double sample_ratio) const override
    {
        return getRatioOfDefaultRowsImpl<ColumnString>(sample_ratio);
    }

    void getIndicesOfNonDefaultRows(Offsets & indices, size_t from, size_t limit) const override
    {
        return getIndicesOfNonDefaultRowsImpl<ColumnString>(indices, from, limit);
    }

    Chars & getChars() { return chars; }
    const Chars & getChars() const { return chars; }

    Offsets & getOffsets() { return offsets; }
    const Offsets & getOffsets() const { return offsets; }

    // Throws an exception if offsets/chars are messed up
    void validate() const;

    bool isCollationSupported() const override { return true; }
};


}
