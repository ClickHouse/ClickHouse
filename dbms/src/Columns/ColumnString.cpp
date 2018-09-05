#include <Core/Defines.h>
#include <Common/Arena.h>
#include <Columns/Collator.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsCommon.h>
#include <DataStreams/ColumnGathererStream.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int PARAMETER_OUT_OF_BOUND;
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
}


MutableColumnPtr ColumnString::cloneResized(size_t to_size) const
{
    auto res = ColumnString::create();

    if (to_size == 0)
        return res;

    size_t from_size = size();

    if (to_size <= from_size)
    {
        /// Just cut column.

        res->offsets.assign(offsets.begin(), offsets.begin() + to_size);
        res->chars.assign(chars.begin(), chars.begin() + offsets[to_size - 1]);
    }
    else
    {
        /// Copy column and append empty strings for extra elements.

        Offset offset = 0;
        if (from_size > 0)
        {
            res->offsets.assign(offsets.begin(), offsets.end());
            res->chars.assign(chars.begin(), chars.end());
            offset = offsets.back();
        }

        /// Empty strings are just zero terminating bytes.

        res->chars.resize_fill(res->chars.size() + to_size - from_size);

        res->offsets.resize(to_size);
        for (size_t i = from_size; i < to_size; ++i)
        {
            ++offset;
            res->offsets[i] = offset;
        }
    }

    return res;
}


void ColumnString::insertRangeFrom(const IColumn & src, size_t start, size_t length)
{
    if (length == 0)
        return;

    const ColumnString & src_concrete = static_cast<const ColumnString &>(src);

    if (start + length > src_concrete.offsets.size())
        throw Exception("Parameter out of bound in IColumnString::insertRangeFrom method.",
            ErrorCodes::PARAMETER_OUT_OF_BOUND);

    size_t nested_offset = src_concrete.offsetAt(start);
    size_t nested_length = src_concrete.offsets[start + length - 1] - nested_offset;

    size_t old_chars_size = chars.size();
    chars.resize(old_chars_size + nested_length);
    memcpy(&chars[old_chars_size], &src_concrete.chars[nested_offset], nested_length);

    if (start == 0 && offsets.empty())
    {
        offsets.assign(src_concrete.offsets.begin(), src_concrete.offsets.begin() + length);
    }
    else
    {
        size_t old_size = offsets.size();
        size_t prev_max_offset = old_size ? offsets.back() : 0;
        offsets.resize(old_size + length);

        for (size_t i = 0; i < length; ++i)
            offsets[old_size + i] = src_concrete.offsets[start + i] - nested_offset + prev_max_offset;
    }
}


ColumnPtr ColumnString::filter(const Filter & filt, ssize_t result_size_hint) const
{
    if (offsets.size() == 0)
        return ColumnString::create();

    auto res = ColumnString::create();

    Chars_t & res_chars = res->chars;
    Offsets & res_offsets = res->offsets;

    filterArraysImpl<UInt8>(chars, offsets, res_chars, res_offsets, filt, result_size_hint);
    return res;
}


ColumnPtr ColumnString::permute(const Permutation & perm, size_t limit) const
{
    size_t size = offsets.size();

    if (limit == 0)
        limit = size;
    else
        limit = std::min(size, limit);

    if (perm.size() < limit)
        throw Exception("Size of permutation is less than required.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    if (limit == 0)
        return ColumnString::create();

    auto res = ColumnString::create();

    Chars_t & res_chars = res->chars;
    Offsets & res_offsets = res->offsets;

    if (limit == size)
        res_chars.resize(chars.size());
    else
    {
        size_t new_chars_size = 0;
        for (size_t i = 0; i < limit; ++i)
            new_chars_size += sizeAt(perm[i]);
        res_chars.resize(new_chars_size);
    }

    res_offsets.resize(limit);

    Offset current_new_offset = 0;

    for (size_t i = 0; i < limit; ++i)
    {
        size_t j = perm[i];
        size_t string_offset = j == 0 ? 0 : offsets[j - 1];
        size_t string_size = offsets[j] - string_offset;

        memcpySmallAllowReadWriteOverflow15(&res_chars[current_new_offset], &chars[string_offset], string_size);

        current_new_offset += string_size;
        res_offsets[i] = current_new_offset;
    }

    return res;
}


StringRef ColumnString::serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const
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

const char * ColumnString::deserializeAndInsertFromArena(const char * pos)
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


ColumnPtr ColumnString::index(const IColumn & indexes, size_t limit) const
{
    return selectIndexImpl(*this, indexes, limit);
}

template <typename Type>
ColumnPtr ColumnString::indexImpl(const PaddedPODArray<Type> & indexes, size_t limit) const
{
    if (limit == 0)
        return ColumnString::create();

    auto res = ColumnString::create();

    Chars_t & res_chars = res->chars;
    Offsets & res_offsets = res->offsets;

    size_t new_chars_size = 0;
    for (size_t i = 0; i < limit; ++i)
        new_chars_size += sizeAt(indexes[i]);
    res_chars.resize(new_chars_size);

    res_offsets.resize(limit);

    Offset current_new_offset = 0;

    for (size_t i = 0; i < limit; ++i)
    {
        size_t j = indexes[i];
        size_t string_offset = j == 0 ? 0 : offsets[j - 1];
        size_t string_size = offsets[j] - string_offset;

        memcpySmallAllowReadWriteOverflow15(&res_chars[current_new_offset], &chars[string_offset], string_size);

        current_new_offset += string_size;
        res_offsets[i] = current_new_offset;
    }

    return res;
}


template <bool positive>
struct ColumnString::less
{
    const ColumnString & parent;
    explicit less(const ColumnString & parent_) : parent(parent_) {}
    bool operator()(size_t lhs, size_t rhs) const
    {
        size_t left_len = parent.sizeAt(lhs);
        size_t right_len = parent.sizeAt(rhs);

        int res = memcmp(&parent.chars[parent.offsetAt(lhs)], &parent.chars[parent.offsetAt(rhs)], std::min(left_len, right_len));

        if (res != 0)
            return positive ? (res < 0) : (res > 0);
        else
            return positive ? (left_len < right_len) : (left_len > right_len);
    }
};

void ColumnString::getPermutation(bool reverse, size_t limit, int /*nan_direction_hint*/, Permutation & res) const
{
    size_t s = offsets.size();
    res.resize(s);
    for (size_t i = 0; i < s; ++i)
        res[i] = i;

    if (limit >= s)
        limit = 0;

    if (limit)
    {
        if (reverse)
            std::partial_sort(res.begin(), res.begin() + limit, res.end(), less<false>(*this));
        else
            std::partial_sort(res.begin(), res.begin() + limit, res.end(), less<true>(*this));
    }
    else
    {
        if (reverse)
            std::sort(res.begin(), res.end(), less<false>(*this));
        else
            std::sort(res.begin(), res.end(), less<true>(*this));
    }
}


ColumnPtr ColumnString::replicate(const Offsets & replicate_offsets) const
{
    size_t col_size = size();
    if (col_size != replicate_offsets.size())
        throw Exception("Size of offsets doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    auto res = ColumnString::create();

    if (0 == col_size)
        return res;

    Chars_t & res_chars = res->chars;
    Offsets & res_offsets = res->offsets;
    res_chars.reserve(chars.size() / col_size * replicate_offsets.back());
    res_offsets.reserve(replicate_offsets.back());

    Offset prev_replicate_offset = 0;
    Offset prev_string_offset = 0;
    Offset current_new_offset = 0;

    for (size_t i = 0; i < col_size; ++i)
    {
        size_t size_to_replicate = replicate_offsets[i] - prev_replicate_offset;
        size_t string_size = offsets[i] - prev_string_offset;

        for (size_t j = 0; j < size_to_replicate; ++j)
        {
            current_new_offset += string_size;
            res_offsets.push_back(current_new_offset);

            res_chars.resize(res_chars.size() + string_size);
            memcpySmallAllowReadWriteOverflow15(
                &res_chars[res_chars.size() - string_size], &chars[prev_string_offset], string_size);
        }

        prev_replicate_offset = replicate_offsets[i];
        prev_string_offset = offsets[i];
    }

    return res;
}


void ColumnString::gather(ColumnGathererStream & gatherer)
{
    gatherer.gather(*this);
}


void ColumnString::reserve(size_t n)
{
    offsets.reserve(n);
}


void ColumnString::getExtremes(Field & min, Field & max) const
{
    min = String();
    max = String();

    size_t col_size = size();

    if (col_size == 0)
        return;

    size_t min_idx = 0;
    size_t max_idx = 0;

    less<true> less_op(*this);

    for (size_t i = 1; i < col_size; ++i)
    {
        if (less_op(i, min_idx))
            min_idx = i;
        else if (less_op(max_idx, i))
            max_idx = i;
    }

    get(min_idx, min);
    get(max_idx, max);
}


int ColumnString::compareAtWithCollation(size_t n, size_t m, const IColumn & rhs_, const Collator & collator) const
{
    const ColumnString & rhs = static_cast<const ColumnString &>(rhs_);

    return collator.compare(
        reinterpret_cast<const char *>(&chars[offsetAt(n)]), sizeAt(n),
        reinterpret_cast<const char *>(&rhs.chars[rhs.offsetAt(m)]), rhs.sizeAt(m));
}


template <bool positive>
struct ColumnString::lessWithCollation
{
    const ColumnString & parent;
    const Collator & collator;

    lessWithCollation(const ColumnString & parent_, const Collator & collator_) : parent(parent_), collator(collator_) {}

    bool operator()(size_t lhs, size_t rhs) const
    {
        int res = collator.compare(
            reinterpret_cast<const char *>(&parent.chars[parent.offsetAt(lhs)]), parent.sizeAt(lhs),
            reinterpret_cast<const char *>(&parent.chars[parent.offsetAt(rhs)]), parent.sizeAt(rhs));

        return positive ? (res < 0) : (res > 0);
    }
};

void ColumnString::getPermutationWithCollation(const Collator & collator, bool reverse, size_t limit, Permutation & res) const
{
    size_t s = offsets.size();
    res.resize(s);
    for (size_t i = 0; i < s; ++i)
        res[i] = i;

    if (limit >= s)
        limit = 0;

    if (limit)
    {
        if (reverse)
            std::partial_sort(res.begin(), res.begin() + limit, res.end(), lessWithCollation<false>(*this, collator));
        else
            std::partial_sort(res.begin(), res.begin() + limit, res.end(), lessWithCollation<true>(*this, collator));
    }
    else
    {
        if (reverse)
            std::sort(res.begin(), res.end(), lessWithCollation<false>(*this, collator));
        else
            std::sort(res.begin(), res.end(), lessWithCollation<true>(*this, collator));
    }
}

}
