#pragma once

#include <string.h> // memcpy

#include <Common/PODArray.h>
#include <Columns/IColumn.h>


namespace DB
{

/** A column of values of "fixed-length string" type.
  * If you insert a smaller string, it will be padded with zero bytes.
  */
class ColumnFixedString final : public COWPtrHelper<IColumn, ColumnFixedString>
{
public:
    friend class COWPtrHelper<IColumn, ColumnFixedString>;

    using Chars_t = PaddedPODArray<UInt8>;

private:
    /// Bytes of rows, laid in succession. The strings are stored without a trailing zero byte.
    /** NOTE It is required that the offset and type of chars in the object be the same as that of `data in ColumnUInt8`.
      * Used in `packFixed` function (AggregationCommon.h)
      */
    Chars_t chars;
    /// The size of the rows.
    const size_t n;

    template <bool positive>
    struct less;

    /** Create an empty column of strings of fixed-length `n` */
    ColumnFixedString(size_t n_) : n(n_) {}

    ColumnFixedString(const ColumnFixedString & src) : chars(src.chars.begin(), src.chars.end()), n(src.n) {};

public:
    std::string getName() const override { return "FixedString(" + std::to_string(n) + ")"; }
    const char * getFamilyName() const override { return "FixedString"; }

    MutableColumnPtr cloneResized(size_t size) const override;

    size_t size() const override
    {
        return chars.size() / n;
    }

    size_t byteSize() const override
    {
        return chars.size() + sizeof(n);
    }

    size_t allocatedBytes() const override
    {
        return chars.allocated_bytes() + sizeof(n);
    }

    Field operator[](size_t index) const override
    {
        return String(reinterpret_cast<const char *>(&chars[n * index]), n);
    }

    void get(size_t index, Field & res) const override
    {
        res.assignString(reinterpret_cast<const char *>(&chars[n * index]), n);
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

    void popBack(size_t elems) override
    {
        chars.resize_assume_reserved(chars.size() - n * elems);
    }

    StringRef serializeValueIntoArena(size_t index, Arena & arena, char const *& begin) const override;

    const char * deserializeAndInsertFromArena(const char * pos) override;

    void updateHashWithValue(size_t index, SipHash & hash) const override;

    int compareAt(size_t p1, size_t p2, const IColumn & rhs_, int /*nan_direction_hint*/) const override
    {
        const ColumnFixedString & rhs = static_cast<const ColumnFixedString &>(rhs_);
        return memcmp(&chars[p1 * n], &rhs.chars[p2 * n], n);
    }

    void getPermutation(bool reverse, size_t limit, int nan_direction_hint, Permutation & res) const override;

    void insertRangeFrom(const IColumn & src, size_t start, size_t length) override;

    ColumnPtr filter(const IColumn::Filter & filt, ssize_t result_size_hint) const override;

    ColumnPtr permute(const Permutation & perm, size_t limit) const override;

    ColumnPtr replicate(const Offsets & offsets) const override;

    MutableColumns scatter(ColumnIndex num_columns, const Selector & selector) const override
    {
        return scatterImpl<ColumnFixedString>(num_columns, selector);
    }

    void gather(ColumnGathererStream & gatherer_stream) override;

    void reserve(size_t size) override
    {
        chars.reserve(n * size);
    };

    void getExtremes(Field & min, Field & max) const override;


    bool canBeInsideNullable() const override { return true; }

    bool isFixedAndContiguous() const override { return true; }
    size_t sizeOfValueIfFixed() const override { return n; }
    StringRef getRawData() const override { return StringRef(chars.data(), chars.size()); }

    /// Specialized part of interface, not from IColumn.

    Chars_t & getChars() { return chars; }
    const Chars_t & getChars() const { return chars; }

    size_t getN() const { return n; }
};


}
