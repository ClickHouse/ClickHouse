#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnCompressed.h>

#include <DataStreams/ColumnGathererStream.h>
#include <IO/WriteHelpers.h>
#include <Common/Arena.h>
#include <Common/HashTable/Hash.h>
#include <Common/SipHash.h>
#include <Common/WeakHash.h>
#include <Common/assert_cast.h>
#include <Common/memcmpSmall.h>
#include <Common/memcpySmall.h>
#include <common/sort.h>
#include <ext/scope_guard.h>

#if defined(__SSE2__)
#    include <emmintrin.h>
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_LARGE_STRING_SIZE;
    extern const int SIZE_OF_FIXED_STRING_DOESNT_MATCH;
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
    extern const int PARAMETER_OUT_OF_BOUND;
    extern const int LOGICAL_ERROR;
}


MutableColumnPtr ColumnFixedString::cloneResized(size_t size) const
{
    MutableColumnPtr new_col_holder = ColumnFixedString::create(n);

    if (size > 0)
    {
        auto & new_col = assert_cast<ColumnFixedString &>(*new_col_holder);
        new_col.chars.resize(size * n);

        size_t count = std::min(this->size(), size);
        memcpy(new_col.chars.data(), chars.data(), count * n * sizeof(chars[0]));

        if (size > count)
            memset(&(new_col.chars[count * n]), '\0', (size - count) * n);
    }

    return new_col_holder;
}

void ColumnFixedString::insert(const Field & x)
{
    const String & s = DB::get<const String &>(x);

    if (s.size() > n)
        throw Exception("Too large string '" + s + "' for FixedString column", ErrorCodes::TOO_LARGE_STRING_SIZE);

    size_t old_size = chars.size();
    chars.resize_fill(old_size + n);
    memcpy(chars.data() + old_size, s.data(), s.size());
}

void ColumnFixedString::insertFrom(const IColumn & src_, size_t index)
{
    const ColumnFixedString & src = assert_cast<const ColumnFixedString &>(src_);

    if (n != src.getN())
        throw Exception("Size of FixedString doesn't match", ErrorCodes::SIZE_OF_FIXED_STRING_DOESNT_MATCH);

    size_t old_size = chars.size();
    chars.resize(old_size + n);
    memcpySmallAllowReadWriteOverflow15(chars.data() + old_size, &src.chars[n * index], n);
}

void ColumnFixedString::insertData(const char * pos, size_t length)
{
    if (length > n)
        throw Exception("Too large string for FixedString column", ErrorCodes::TOO_LARGE_STRING_SIZE);

    size_t old_size = chars.size();
    chars.resize_fill(old_size + n);
    memcpy(chars.data() + old_size, pos, length);
}

StringRef ColumnFixedString::serializeValueIntoArena(size_t index, Arena & arena, char const *& begin) const
{
    auto * pos = arena.allocContinue(n, begin);
    memcpy(pos, &chars[n * index], n);
    return StringRef(pos, n);
}

const char * ColumnFixedString::deserializeAndInsertFromArena(const char * pos)
{
    size_t old_size = chars.size();
    chars.resize(old_size + n);
    memcpy(chars.data() + old_size, pos, n);
    return pos + n;
}

void ColumnFixedString::updateHashWithValue(size_t index, SipHash & hash) const
{
    hash.update(reinterpret_cast<const char *>(&chars[n * index]), n);
}

void ColumnFixedString::updateWeakHash32(WeakHash32 & hash) const
{
    auto s = size();

    if (hash.getData().size() != s)
        throw Exception("Size of WeakHash32 does not match size of column: column size is " + std::to_string(s) +
                        ", hash size is " + std::to_string(hash.getData().size()), ErrorCodes::LOGICAL_ERROR);

    const UInt8 * pos = chars.data();
    UInt32 * hash_data = hash.getData().data();

    for (size_t row = 0; row < s; ++row)
    {
        *hash_data = ::updateWeakHash32(pos, n, *hash_data);

        pos += n;
        ++hash_data;
    }
}

void ColumnFixedString::updateHashFast(SipHash & hash) const
{
    hash.update(n);
    hash.update(reinterpret_cast<const char *>(chars.data()), size() * n);
}

template <bool positive>
struct ColumnFixedString::less
{
    const ColumnFixedString & parent;
    explicit less(const ColumnFixedString & parent_) : parent(parent_) {}
    bool operator()(size_t lhs, size_t rhs) const
    {
        int res = memcmpSmallAllowOverflow15(parent.chars.data() + lhs * parent.n, parent.chars.data() + rhs * parent.n, parent.n);
        return positive ? (res < 0) : (res > 0);
    }
};

void ColumnFixedString::getPermutation(bool reverse, size_t limit, int /*nan_direction_hint*/, Permutation & res) const
{
    size_t s = size();
    res.resize(s);
    for (size_t i = 0; i < s; ++i)
        res[i] = i;

    if (limit >= s)
        limit = 0;

    if (limit)
    {
        if (reverse)
            partial_sort(res.begin(), res.begin() + limit, res.end(), less<false>(*this));
        else
            partial_sort(res.begin(), res.begin() + limit, res.end(), less<true>(*this));
    }
    else
    {
        if (reverse)
            std::sort(res.begin(), res.end(), less<false>(*this));
        else
            std::sort(res.begin(), res.end(), less<true>(*this));
    }
}

void ColumnFixedString::updatePermutation(bool reverse, size_t limit, int, Permutation & res, EqualRanges & equal_ranges) const
{
    if (equal_ranges.empty())
        return;

    if (limit >= size() || limit >= equal_ranges.back().second)
        limit = 0;

    size_t number_of_ranges = equal_ranges.size();
    if (limit)
        --number_of_ranges;

    EqualRanges new_ranges;
    SCOPE_EXIT({equal_ranges = std::move(new_ranges);});

    for (size_t i = 0; i < number_of_ranges; ++i)
    {
        const auto& [first, last] = equal_ranges[i];
        if (reverse)
            std::sort(res.begin() + first, res.begin() + last, less<false>(*this));
        else
            std::sort(res.begin() + first, res.begin() + last, less<true>(*this));

        auto new_first = first;
        for (auto j = first + 1; j < last; ++j)
        {
            if (memcmpSmallAllowOverflow15(chars.data() + res[j] * n, chars.data() + res[new_first] * n, n) != 0)
            {
                if (j - new_first > 1)
                    new_ranges.emplace_back(new_first, j);

                new_first = j;
            }
        }
        if (last - new_first > 1)
            new_ranges.emplace_back(new_first, last);
    }
    if (limit)
    {
        const auto & [first, last] = equal_ranges.back();

        if (limit < first || limit > last)
            return;

        /// Since then we are working inside the interval.

        if (reverse)
            partial_sort(res.begin() + first, res.begin() + limit, res.begin() + last, less<false>(*this));
        else
            partial_sort(res.begin() + first, res.begin() + limit, res.begin() + last, less<true>(*this));

        auto new_first = first;
        for (auto j = first + 1; j < limit; ++j)
        {
            if (memcmpSmallAllowOverflow15(chars.data() + res[j] * n, chars.data() + res[new_first] * n, n)  != 0)
            {
                if (j - new_first > 1)
                    new_ranges.emplace_back(new_first, j);

                new_first = j;
            }
        }
        auto new_last = limit;
        for (auto j = limit; j < last; ++j)
        {
            if (memcmpSmallAllowOverflow15(chars.data() + res[j] * n, chars.data() + res[new_first] * n, n)  == 0)
            {
                std::swap(res[new_last], res[j]);
                ++new_last;
            }
        }
        if (new_last - new_first > 1)
            new_ranges.emplace_back(new_first, new_last);
    }
}

void ColumnFixedString::insertRangeFrom(const IColumn & src, size_t start, size_t length)
{
    const ColumnFixedString & src_concrete = assert_cast<const ColumnFixedString &>(src);

    if (start + length > src_concrete.size())
        throw Exception("Parameters start = "
            + toString(start) + ", length = "
            + toString(length) + " are out of bound in ColumnFixedString::insertRangeFrom method"
            " (size() = " + toString(src_concrete.size()) + ").",
            ErrorCodes::PARAMETER_OUT_OF_BOUND);

    size_t old_size = chars.size();
    chars.resize(old_size + length * n);
    memcpy(chars.data() + old_size, &src_concrete.chars[start * n], length * n);
}

ColumnPtr ColumnFixedString::filter(const IColumn::Filter & filt, ssize_t result_size_hint) const
{
    size_t col_size = size();
    if (col_size != filt.size())
        throw Exception("Size of filter doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    auto res = ColumnFixedString::create(n);

    if (result_size_hint)
        res->chars.reserve(result_size_hint > 0 ? result_size_hint * n : chars.size());

    const UInt8 * filt_pos = filt.data();
    const UInt8 * filt_end = filt_pos + col_size;
    const UInt8 * data_pos = chars.data();

#ifdef __SSE2__
    /** A slightly more optimized version.
        * Based on the assumption that often pieces of consecutive values
        *  completely pass or do not pass the filter.
        * Therefore, we will optimistically check the parts of `SIMD_BYTES` values.
        */

    static constexpr size_t SIMD_BYTES = 16;
    const __m128i zero16 = _mm_setzero_si128();
    const UInt8 * filt_end_sse = filt_pos + col_size / SIMD_BYTES * SIMD_BYTES;
    const size_t chars_per_simd_elements = SIMD_BYTES * n;

    while (filt_pos < filt_end_sse)
    {
        UInt16 mask = _mm_movemask_epi8(_mm_cmpeq_epi8(_mm_loadu_si128(reinterpret_cast<const __m128i *>(filt_pos)), zero16));
        mask = ~mask;

        if (0 == mask)
        {
            /// Nothing is inserted.
            data_pos += chars_per_simd_elements;
        }
        else if (0xFFFF == mask)
        {
            res->chars.insert(data_pos, data_pos + chars_per_simd_elements);
            data_pos += chars_per_simd_elements;
        }
        else
        {
            size_t res_chars_size = res->chars.size();
            for (size_t i = 0; i < SIMD_BYTES; ++i)
            {
                if (filt_pos[i])
                {
                    res->chars.resize(res_chars_size + n);
                    memcpySmallAllowReadWriteOverflow15(&res->chars[res_chars_size], data_pos, n);
                    res_chars_size += n;
                }
                data_pos += n;
            }
        }

        filt_pos += SIMD_BYTES;
    }
#endif

    size_t res_chars_size = res->chars.size();
    while (filt_pos < filt_end)
    {
        if (*filt_pos)
        {
            res->chars.resize(res_chars_size + n);
            memcpySmallAllowReadWriteOverflow15(&res->chars[res_chars_size], data_pos, n);
            res_chars_size += n;
        }

        ++filt_pos;
        data_pos += n;
    }

    return res;
}

ColumnPtr ColumnFixedString::permute(const Permutation & perm, size_t limit) const
{
    size_t col_size = size();

    if (limit == 0)
        limit = col_size;
    else
        limit = std::min(col_size, limit);

    if (perm.size() < limit)
        throw Exception("Size of permutation is less than required.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    if (limit == 0)
        return ColumnFixedString::create(n);

    auto res = ColumnFixedString::create(n);

    Chars & res_chars = res->chars;

    res_chars.resize(n * limit);

    size_t offset = 0;
    for (size_t i = 0; i < limit; ++i, offset += n)
        memcpySmallAllowReadWriteOverflow15(&res_chars[offset], &chars[perm[i] * n], n);

    return res;
}


ColumnPtr ColumnFixedString::index(const IColumn & indexes, size_t limit) const
{
    return selectIndexImpl(*this, indexes, limit);
}


template <typename Type>
ColumnPtr ColumnFixedString::indexImpl(const PaddedPODArray<Type> & indexes, size_t limit) const
{
    if (limit == 0)
        return ColumnFixedString::create(n);

    auto res = ColumnFixedString::create(n);

    Chars & res_chars = res->chars;

    res_chars.resize(n * limit);

    size_t offset = 0;
    for (size_t i = 0; i < limit; ++i, offset += n)
        memcpySmallAllowReadWriteOverflow15(&res_chars[offset], &chars[indexes[i] * n], n);

    return res;
}

ColumnPtr ColumnFixedString::replicate(const Offsets & offsets) const
{
    size_t col_size = size();
    if (col_size != offsets.size())
        throw Exception("Size of offsets doesn't match size of column.", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    auto res = ColumnFixedString::create(n);

    if (0 == col_size)
        return res;

    Chars & res_chars = res->chars;
    res_chars.resize(n * offsets.back());

    Offset curr_offset = 0;
    for (size_t i = 0; i < col_size; ++i)
        for (size_t next_offset = offsets[i]; curr_offset < next_offset; ++curr_offset)
            memcpySmallAllowReadWriteOverflow15(&res->chars[curr_offset * n], &chars[i * n], n);

    return res;
}

void ColumnFixedString::gather(ColumnGathererStream & gatherer)
{
    gatherer.gather(*this);
}

void ColumnFixedString::getExtremes(Field & min, Field & max) const
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

ColumnPtr ColumnFixedString::compress() const
{
    size_t source_size = chars.size();

    /// Don't compress small blocks.
    if (source_size < 4096) /// A wild guess.
        return ColumnCompressed::wrap(this->getPtr());

    auto compressed = ColumnCompressed::compressBuffer(chars.data(), source_size, false);

    if (!compressed)
        return ColumnCompressed::wrap(this->getPtr());

    size_t column_size = size();

    return ColumnCompressed::create(column_size, compressed->size(),
        [compressed = std::move(compressed), column_size, n = n]
        {
            size_t chars_size = n * column_size;
            auto res = ColumnFixedString::create(n);
            res->getChars().resize(chars_size);
            ColumnCompressed::decompressBuffer(
                compressed->data(), res->getChars().data(), compressed->size(), chars_size);
            return res;
        });
}

}
