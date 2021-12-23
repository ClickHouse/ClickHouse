#include <Columns/ColumnString.h>

#include <Columns/Collator.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnCompressed.h>
#include <Columns/MaskOperations.h>
#include <Processors/Transforms/ColumnGathererTransform.h>
#include <Common/Arena.h>
#include <Common/HashTable/Hash.h>
#include <Common/WeakHash.h>
#include <Common/assert_cast.h>
#include <Common/memcmpSmall.h>
#include <base/sort.h>
#include <base/unaligned.h>
#include <base/scope_guard.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int PARAMETER_OUT_OF_BOUND;
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
    extern const int LOGICAL_ERROR;
}


ColumnString::ColumnString(const ColumnString & src)
    : COWHelper<IColumn, ColumnString>(src),
    offsets(src.offsets.begin(), src.offsets.end()),
    chars(src.chars.begin(), src.chars.end())
{
    if (!offsets.empty())
    {
        Offset last_offset = offsets.back();

        /// This will also prevent possible overflow in offset.
        if (chars.size() != last_offset)
            throw Exception("String offsets has data inconsistent with chars array", ErrorCodes::LOGICAL_ERROR);
    }
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

void ColumnString::updateWeakHash32(WeakHash32 & hash) const
{
    auto s = offsets.size();

    if (hash.getData().size() != s)
        throw Exception("Size of WeakHash32 does not match size of column: column size is " + std::to_string(s) +
                        ", hash size is " + std::to_string(hash.getData().size()), ErrorCodes::LOGICAL_ERROR);

    const UInt8 * pos = chars.data();
    UInt32 * hash_data = hash.getData().data();
    Offset prev_offset = 0;

    for (const auto & offset : offsets)
    {
        auto str_size = offset - prev_offset;
        /// Skip last zero byte.
        *hash_data = ::updateWeakHash32(pos, str_size - 1, *hash_data);

        pos += str_size;
        prev_offset = offset;
        ++hash_data;
    }
}


void ColumnString::insertRangeFrom(const IColumn & src, size_t start, size_t length)
{
    if (length == 0)
        return;

    const ColumnString & src_concrete = assert_cast<const ColumnString &>(src);

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
        size_t prev_max_offset = offsets.back();    /// -1th index is Ok, see PaddedPODArray
        offsets.resize(old_size + length);

        for (size_t i = 0; i < length; ++i)
            offsets[old_size + i] = src_concrete.offsets[start + i] - nested_offset + prev_max_offset;
    }
}


ColumnPtr ColumnString::filter(const Filter & filt, ssize_t result_size_hint) const
{
    if (offsets.empty())
        return ColumnString::create();

    auto res = ColumnString::create();

    Chars & res_chars = res->chars;
    Offsets & res_offsets = res->offsets;

    filterArraysImpl<UInt8>(chars, offsets, res_chars, res_offsets, filt, result_size_hint);
    return res;
}

void ColumnString::expand(const IColumn::Filter & mask, bool inverted)
{
    auto & offsets_data = getOffsets();
    auto & chars_data = getChars();
    if (mask.size() < offsets_data.size())
        throw Exception("Mask size should be no less than data size.", ErrorCodes::LOGICAL_ERROR);

    /// We cannot change only offsets, because each string should end with terminating zero byte.
    /// So, we will insert one zero byte when mask value is zero.

    int index = mask.size() - 1;
    int from = offsets_data.size() - 1;
    /// mask.size() - offsets_data.size() should be equal to the number of zeros in mask
    /// (if not, one of exceptions below will throw) and we can calculate the resulting chars size.
    UInt64 last_offset = offsets_data[from] + (mask.size() - offsets_data.size());
    offsets_data.resize(mask.size());
    chars_data.resize_fill(last_offset, 0);
    while (index >= 0)
    {
        offsets_data[index] = last_offset;
        if (!!mask[index] ^ inverted)
        {
            if (from < 0)
                throw Exception("Too many bytes in mask", ErrorCodes::LOGICAL_ERROR);

            size_t len = offsets_data[from] - offsets_data[from - 1];

            /// Copy only if it makes sense. It's important to copy backward, because
            /// ranges can overlap, but destination is always is more to the right then source
            if (last_offset - len != offsets_data[from - 1])
                std::copy_backward(&chars_data[offsets_data[from - 1]], &chars_data[offsets_data[from]], &chars_data[last_offset]);
            last_offset -= len;
            --from;
        }
        else
        {
            chars_data[last_offset - 1] = 0;
            --last_offset;
        }

        --index;
    }

    if (from != -1)
        throw Exception("Not enough bytes in mask", ErrorCodes::LOGICAL_ERROR);
}


ColumnPtr ColumnString::permute(const Permutation & perm, size_t limit) const
{
    return permuteImpl(*this, perm, limit);
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
    const size_t string_size = unalignedLoad<size_t>(pos);
    pos += sizeof(string_size);

    const size_t old_size = chars.size();
    const size_t new_size = old_size + string_size;
    chars.resize(new_size);
    memcpy(chars.data() + old_size, pos, string_size);

    offsets.push_back(new_size);
    return pos + string_size;
}

const char * ColumnString::skipSerializedInArena(const char * pos) const
{
    const size_t string_size = unalignedLoad<size_t>(pos);
    pos += sizeof(string_size);
    return pos + string_size;
}

ColumnPtr ColumnString::index(const IColumn & indexes, size_t limit) const
{
    return selectIndexImpl(*this, indexes, limit);
}

template <typename Type>
ColumnPtr ColumnString::indexImpl(const PaddedPODArray<Type> & indexes, size_t limit) const
{
    assert(limit <= indexes.size());
    if (limit == 0)
        return ColumnString::create();

    auto res = ColumnString::create();

    Chars & res_chars = res->chars;
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
        size_t string_offset = offsets[j - 1];
        size_t string_size = offsets[j] - string_offset;

        memcpySmallAllowReadWriteOverflow15(&res_chars[current_new_offset], &chars[string_offset], string_size);

        current_new_offset += string_size;
        res_offsets[i] = current_new_offset;
    }

    return res;
}

void ColumnString::compareColumn(
    const IColumn & rhs, size_t rhs_row_num,
    PaddedPODArray<UInt64> * row_indexes, PaddedPODArray<Int8> & compare_results,
    int direction, int nan_direction_hint) const
{
    return doCompareColumn<ColumnString>(assert_cast<const ColumnString &>(rhs), rhs_row_num, row_indexes,
                                         compare_results, direction, nan_direction_hint);
}

bool ColumnString::hasEqualValues() const
{
    return hasEqualValuesImpl<ColumnString>();
}

template <bool positive>
struct ColumnString::Cmp
{
    const ColumnString & parent;
    explicit Cmp(const ColumnString & parent_) : parent(parent_) {}
    int operator()(size_t lhs, size_t rhs) const
    {
        int res = memcmpSmallAllowOverflow15(
            parent.chars.data() + parent.offsetAt(lhs), parent.sizeAt(lhs) - 1,
            parent.chars.data() + parent.offsetAt(rhs), parent.sizeAt(rhs) - 1);

        if constexpr (positive)
            return res;
        else
            return -res;
    }
};

template <typename Comparator>
void ColumnString::getPermutationImpl(size_t limit, Permutation & res, Comparator cmp) const
{
    size_t s = offsets.size();
    res.resize(s);
    for (size_t i = 0; i < s; ++i)
        res[i] = i;

    if (limit >= s)
        limit = 0;

    auto less = [&cmp](size_t lhs, size_t rhs){ return cmp(lhs, rhs) < 0; };

    if (limit)
        partial_sort(res.begin(), res.begin() + limit, res.end(), less);
    else
        std::sort(res.begin(), res.end(), less);
}

void ColumnString::getPermutation(bool reverse, size_t limit, int /*nan_direction_hint*/, Permutation & res) const
{
    if (reverse)
        getPermutationImpl(limit, res, Cmp<false>(*this));
    else
        getPermutationImpl(limit, res, Cmp<true>(*this));
}

void ColumnString::updatePermutation(bool reverse, size_t limit, int /*nan_direction_hint*/, Permutation & res, EqualRanges & equal_ranges) const
{
    if (reverse)
        updatePermutationImpl(limit, res, equal_ranges, Cmp<false>(*this));
    else
        updatePermutationImpl(limit, res, equal_ranges, Cmp<true>(*this));
}

template <bool positive>
struct ColumnString::CmpWithCollation
{
    const ColumnString & parent;
    const Collator & collator;

    CmpWithCollation(const ColumnString & parent_, const Collator & collator_) : parent(parent_), collator(collator_) {}

    int operator()(size_t lhs, size_t rhs) const
    {
        int res = collator.compare(
            reinterpret_cast<const char *>(&parent.chars[parent.offsetAt(lhs)]), parent.sizeAt(lhs),
            reinterpret_cast<const char *>(&parent.chars[parent.offsetAt(rhs)]), parent.sizeAt(rhs));

        return positive ? res : -res;
    }
};

void ColumnString::getPermutationWithCollation(const Collator & collator, bool reverse, size_t limit, int, Permutation & res) const
{
    if (reverse)
        getPermutationImpl(limit, res, CmpWithCollation<false>(*this, collator));
    else
        getPermutationImpl(limit, res, CmpWithCollation<true>(*this, collator));
}

void ColumnString::updatePermutationWithCollation(const Collator & collator, bool reverse, size_t limit, int, Permutation & res, EqualRanges & equal_ranges) const
{
    if (reverse)
        updatePermutationImpl(limit, res, equal_ranges, CmpWithCollation<false>(*this, collator));
    else
        updatePermutationImpl(limit, res, equal_ranges, CmpWithCollation<true>(*this, collator));
}

ColumnPtr ColumnString::replicate(const Offsets & replicate_offsets) const
{
    size_t col_size = size();
    if (col_size != replicate_offsets.size())
        throw Exception("Size of offsets doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    auto res = ColumnString::create();

    if (0 == col_size)
        return res;

    Chars & res_chars = res->chars;
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

    Cmp<true> cmp_op(*this);

    for (size_t i = 1; i < col_size; ++i)
    {
        if (cmp_op(i, min_idx) < 0)
            min_idx = i;
        else if (cmp_op(max_idx, i) < 0)
            max_idx = i;
    }

    get(min_idx, min);
    get(max_idx, max);
}

ColumnPtr ColumnString::compress() const
{
    size_t source_chars_size = chars.size();
    size_t source_offsets_size = offsets.size() * sizeof(Offset);

    /// Don't compress small blocks.
    if (source_chars_size < 4096) /// A wild guess.
        return ColumnCompressed::wrap(this->getPtr());

    auto chars_compressed = ColumnCompressed::compressBuffer(chars.data(), source_chars_size, false);

    /// Return original column if not compressible.
    if (!chars_compressed)
        return ColumnCompressed::wrap(this->getPtr());

    auto offsets_compressed = ColumnCompressed::compressBuffer(offsets.data(), source_offsets_size, true);

    return ColumnCompressed::create(offsets.size(), chars_compressed->size() + offsets_compressed->size(),
        [
            chars_compressed = std::move(chars_compressed),
            offsets_compressed = std::move(offsets_compressed),
            source_chars_size,
            source_offsets_elements = offsets.size()
        ]
        {
            auto res = ColumnString::create();

            res->getChars().resize(source_chars_size);
            res->getOffsets().resize(source_offsets_elements);

            ColumnCompressed::decompressBuffer(
                chars_compressed->data(), res->getChars().data(), chars_compressed->size(), source_chars_size);

            ColumnCompressed::decompressBuffer(
                offsets_compressed->data(), res->getOffsets().data(), offsets_compressed->size(), source_offsets_elements * sizeof(Offset));

            return res;
        });
}


int ColumnString::compareAtWithCollation(size_t n, size_t m, const IColumn & rhs_, int, const Collator & collator) const
{
    const ColumnString & rhs = assert_cast<const ColumnString &>(rhs_);

    return collator.compare(
        reinterpret_cast<const char *>(&chars[offsetAt(n)]), sizeAt(n),
        reinterpret_cast<const char *>(&rhs.chars[rhs.offsetAt(m)]), rhs.sizeAt(m));
}

void ColumnString::protect()
{
    getChars().protect();
    getOffsets().protect();
}

void ColumnString::validate() const
{
    if (!offsets.empty() && offsets.back() != chars.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ColumnString validation failed: size mismatch (internal logical error) {} != {}", offsets.back(), chars.size());
}

}
