#pragma once

#include <Common/PODArray.h>
#include <Common/memcmpSmall.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>
#include <Columns/IColumn.h>
#include <Columns/IColumnImpl.h>
#include <Columns/ColumnVectorHelper.h>
#include <Core/Field.h>


namespace DB
{

/** A column of values of "fixed-length string" type.
  * If you insert a smaller string, it will be padded with zero bytes.
  */
class ColumnFixedString final : public COWHelper<ColumnVectorHelper, ColumnFixedString>
{
public:
    friend class COWHelper<ColumnVectorHelper, ColumnFixedString>;

    using Chars = PaddedPODArray<UInt8>;

private:
    /// Bytes of rows, laid in succession. The strings are stored without a trailing zero byte.
    /** NOTE It is required that the offset and type of chars in the object be the same as that of `data in ColumnUInt8`.
      * Used in `packFixed` function (AggregationCommon.h)
      */
    Chars chars;
    /// The size of the rows.
    const size_t n;

    template <bool positive>
    struct less;

    /** Create an empty column of strings of fixed-length `n` */
    ColumnFixedString(size_t n_) : n(n_) {}

    ColumnFixedString(const ColumnFixedString & src) : chars(src.chars.begin(), src.chars.end()), n(src.n) {}

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

    void insert(const Field & x) override;

    void insertFrom(const IColumn & src_, size_t index) override;

    void insertData(const char * pos, size_t length) override;

    void insertDefault() override
    {
        chars.resize_fill(chars.size() + n);
    }

    virtual void insertManyDefaults(size_t length) override
    {
        chars.resize_fill(chars.size() + n * length);
    }

    void popBack(size_t elems) override
    {
        chars.resize_assume_reserved(chars.size() - n * elems);
    }

    StringRef serializeValueIntoArena(size_t index, Arena & arena, char const *& begin) const override;

    const char * deserializeAndInsertFromArena(const char * pos) override;

    const char * skipSerializedInArena(const char * pos) const override;

    void updateHashWithValue(size_t index, SipHash & hash) const override;

    void updateWeakHash32(WeakHash32 & hash) const override;

    void updateHashFast(SipHash & hash) const override;

    int compareAt(size_t p1, size_t p2, const IColumn & rhs_, int /*nan_direction_hint*/) const override
    {
        const ColumnFixedString & rhs = assert_cast<const ColumnFixedString &>(rhs_);
        return memcmpSmallAllowOverflow15(chars.data() + p1 * n, rhs.chars.data() + p2 * n, n);
    }

    void compareColumn(const IColumn & rhs, size_t rhs_row_num,
                       PaddedPODArray<UInt64> * row_indexes, PaddedPODArray<Int8> & compare_results,
                       int direction, int nan_direction_hint) const override
    {
        return doCompareColumn<ColumnFixedString>(assert_cast<const ColumnFixedString &>(rhs), rhs_row_num, row_indexes,
                                               compare_results, direction, nan_direction_hint);
    }

    bool hasEqualValues() const override
    {
        return hasEqualValuesImpl<ColumnFixedString>();
    }

    void getPermutation(bool reverse, size_t limit, int nan_direction_hint, Permutation & res) const override;

    void updatePermutation(bool reverse, size_t limit, int nan_direction_hint, Permutation & res, EqualRanges & equal_range) const override;

    void insertRangeFrom(const IColumn & src, size_t start, size_t length) override;

    ColumnPtr filter(const IColumn::Filter & filt, ssize_t result_size_hint) const override;

    ColumnPtr permute(const Permutation & perm, size_t limit) const override;

    ColumnPtr index(const IColumn & indexes, size_t limit) const override;

    template <typename Type>
    ColumnPtr indexImpl(const PaddedPODArray<Type> & indexes, size_t limit) const;

    ColumnPtr replicate(const Offsets & offsets) const override;

    MutableColumns scatter(ColumnIndex num_columns, const Selector & selector) const override
    {
        return scatterImpl<ColumnFixedString>(num_columns, selector);
    }

    void gather(ColumnGathererStream & gatherer_stream) override;

    ColumnPtr compress() const override;

    void reserve(size_t size) override
    {
        chars.reserve(n * size);
    }

    void getExtremes(Field & min, Field & max) const override;

    bool structureEquals(const IColumn & rhs) const override
    {
        if (auto rhs_concrete = typeid_cast<const ColumnFixedString *>(&rhs))
            return n == rhs_concrete->n;
        return false;
    }

    bool canBeInsideNullable() const override { return true; }

    bool isFixedAndContiguous() const override { return true; }
    size_t sizeOfValueIfFixed() const override { return n; }
    StringRef getRawData() const override { return StringRef(chars.data(), chars.size()); }

    /// Specialized part of interface, not from IColumn.
    void insertString(const String & string) { insertData(string.c_str(), string.size()); }
    Chars & getChars() { return chars; }
    const Chars & getChars() const { return chars; }

    size_t getN() const { return n; }
};

}
