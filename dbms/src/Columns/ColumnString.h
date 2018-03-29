#pragma once

#include <string.h>

#include <Columns/IColumn.h>
#include <Common/PODArray.h>
#include <Common/Arena.h>
#include <Common/SipHash.h>
#include <Common/memcpySmall.h>


class Collator;


namespace DB
{

/** Column for String values.
  */
class ColumnString final : public COWPtrHelper<IColumn, ColumnString>
{
public:
    using Chars_t = PaddedPODArray<UInt8>;

private:
    friend class COWPtrHelper<IColumn, ColumnString>;

    /// Maps i'th position to offset to i+1'th element. Last offset maps to the end of all chars (is the size of all chars).
    Offsets offsets;

    /// Bytes of strings, placed contiguously.
    /// For convenience, every string ends with terminating zero byte. Note that strings could contain zero bytes in the middle.
    Chars_t chars;

    size_t ALWAYS_INLINE offsetAt(size_t i) const { return i == 0 ? 0 : offsets[i - 1]; }

    /// Size of i-th element, including terminating zero.
    size_t ALWAYS_INLINE sizeAt(size_t i) const { return i == 0 ? offsets[0] : (offsets[i] - offsets[i - 1]); }

    template <bool positive>
    struct less;

    template <bool positive>
    struct lessWithCollation;

    ColumnString() = default;

    ColumnString(const ColumnString & src)
        : offsets(src.offsets.begin(), src.offsets.end()),
        chars(src.chars.begin(), src.chars.end()) {};

public:
    const char * getFamilyName() const override { return "String"; }

    size_t size() const override
    {
        return offsets.size();
    }

    size_t byteSize() const override
    {
        return chars.size() + offsets.size() * sizeof(offsets[0]);
    }

    size_t allocatedBytes() const override
    {
        return chars.allocated_bytes() + offsets.allocated_bytes();
    }

    MutableColumnPtr cloneResized(size_t to_size) const override;

    Field operator[](size_t n) const override
    {
        return Field(&chars[offsetAt(n)], sizeAt(n) - 1);
    }

    void get(size_t n, Field & res) const override
    {
        res.assignString(&chars[offsetAt(n)], sizeAt(n) - 1);
    }

    StringRef getDataAt(size_t n) const override
    {
        return StringRef(&chars[offsetAt(n)], sizeAt(n) - 1);
    }

    StringRef getDataAtWithTerminatingZero(size_t n) const override
    {
        return StringRef(&chars[offsetAt(n)], sizeAt(n));
    }

/// Suppress gcc 7.3.1 warning: '*((void*)&<anonymous> +8)' may be used uninitialized in this function
#if !__clang__
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
        memcpy(&chars[old_size], s.c_str(), size_to_append);
        offsets.push_back(new_size);
    }

#if !__clang__
#pragma GCC diagnostic pop
#endif

        void insertFrom(const IColumn & src_, size_t n) override
    {
        const ColumnString & src = static_cast<const ColumnString &>(src_);

        if (n != 0)
        {
            const size_t size_to_append = src.offsets[n] - src.offsets[n - 1];

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
                memcpySmallAllowReadWriteOverflow15(&chars[old_size], &src.chars[offset], size_to_append);
                offsets.push_back(new_size);
            }
        }
        else
        {
            const size_t old_size = chars.size();
            const size_t size_to_append = src.offsets[0];
            const size_t new_size = old_size + size_to_append;

            chars.resize(new_size);
            memcpySmallAllowReadWriteOverflow15(&chars[old_size], &src.chars[0], size_to_append);
            offsets.push_back(new_size);
        }
    }

    void insertData(const char * pos, size_t length) override
    {
        const size_t old_size = chars.size();
        const size_t new_size = old_size + length + 1;

        chars.resize(new_size);
        memcpy(&chars[old_size], pos, length);
        chars[old_size + length] = 0;
        offsets.push_back(new_size);
    }

    void insertDataWithTerminatingZero(const char * pos, size_t length) override
    {
        const size_t old_size = chars.size();
        const size_t new_size = old_size + length;

        chars.resize(new_size);
        memcpy(&chars[old_size], pos, length);
        offsets.push_back(new_size);
    }

    void popBack(size_t n) override
    {
        size_t nested_n = offsets.back() - offsetAt(offsets.size() - n);
        chars.resize(chars.size() - nested_n);
        offsets.resize_assume_reserved(offsets.size() - n);
    }

    StringRef serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const override
    {
        size_t string_size = sizeAt(n);
        size_t offset = offsetAt(n);

        StringRef res;
        res.size = sizeof(string_size) + string_size;
        char * pos = arena.allocContinue(res.size, begin);
        memcpy(pos, &string_size, sizeof(string_size));
        memcpy(pos + sizeof(string_size), &chars[offset], string_size);
        res.data = pos;

        return res;
    }

    const char * deserializeAndInsertFromArena(const char * pos) override
    {
        const size_t string_size = *reinterpret_cast<const size_t *>(pos);
        pos += sizeof(string_size);

        const size_t old_size = chars.size();
        const size_t new_size = old_size + string_size;
        chars.resize(new_size);
        memcpy(&chars[old_size], pos, string_size);

        offsets.push_back(new_size);
        return pos + string_size;
    }

    void updateHashWithValue(size_t n, SipHash & hash) const override
    {
        size_t string_size = sizeAt(n);
        size_t offset = offsetAt(n);

        hash.update(reinterpret_cast<const char *>(&string_size), sizeof(string_size));
        hash.update(reinterpret_cast<const char *>(&chars[offset]), string_size);
    }

    void insertRangeFrom(const IColumn & src, size_t start, size_t length) override;

    ColumnPtr filter(const Filter & filt, ssize_t result_size_hint) const override;

    ColumnPtr permute(const Permutation & perm, size_t limit) const override;

    void insertDefault() override
    {
        chars.push_back(0);
        offsets.push_back(offsets.size() == 0 ? 1 : (offsets.back() + 1));
    }

    int compareAt(size_t n, size_t m, const IColumn & rhs_, int /*nan_direction_hint*/) const override
    {
        const ColumnString & rhs = static_cast<const ColumnString &>(rhs_);

        const size_t size = sizeAt(n);
        const size_t rhs_size = rhs.sizeAt(m);

        int cmp = memcmp(&chars[offsetAt(n)], &rhs.chars[rhs.offsetAt(m)], std::min(size, rhs_size));

        if (cmp != 0)
            return cmp;
        else
            return size > rhs_size ? 1 : (size < rhs_size ? -1 : 0);
    }

    /// Variant of compareAt for string comparison with respect of collation.
    int compareAtWithCollation(size_t n, size_t m, const IColumn & rhs_, const Collator & collator) const;

    void getPermutation(bool reverse, size_t limit, int nan_direction_hint, Permutation & res) const override;

    /// Sorting with respect of collation.
    void getPermutationWithCollation(const Collator & collator, bool reverse, size_t limit, Permutation & res) const;

    ColumnPtr replicate(const Offsets & replicate_offsets) const override;

    MutableColumns scatter(ColumnIndex num_columns, const Selector & selector) const override
    {
        return scatterImpl<ColumnString>(num_columns, selector);
    }

    void gather(ColumnGathererStream & gatherer_stream) override;

    void reserve(size_t n) override;

    void getExtremes(Field & min, Field & max) const override;


    bool canBeInsideNullable() const override { return true; }


    Chars_t & getChars() { return chars; }
    const Chars_t & getChars() const { return chars; }

    Offsets & getOffsets() { return offsets; }
    const Offsets & getOffsets() const { return offsets; }
};


}
