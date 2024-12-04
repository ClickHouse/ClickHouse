#include <Columns/ColumnString.h>

#include <Columns/Collator.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnCompressed.h>
#include <Columns/MaskOperations.h>
#include <Common/Arena.h>
#include <Common/HashTable/StringHashSet.h>
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
    : COWHelper<IColumnHelper<ColumnString>, ColumnString>(src),
    offsets(src.offsets.begin(), src.offsets.end()),
    chars(src.chars.begin(), src.chars.end())
{
    Offset last_offset = offsets.empty() ? 0 : offsets.back();
    /// This will also prevent possible overflow in offset.
    if (last_offset != chars.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "String offsets has data inconsistent with chars array. Last offset: {}, array length: {}",
            last_offset, chars.size());
}

#if !defined(DEBUG_OR_SANITIZER_BUILD)
void ColumnString::insertManyFrom(const IColumn & src, size_t position, size_t length)
#else
void ColumnString::doInsertManyFrom(const IColumn & src, size_t position, size_t length)
#endif
{
    const ColumnString & src_concrete = assert_cast<const ColumnString &>(src);
    const UInt8 * src_buf = &src_concrete.chars[src_concrete.offsets[position - 1]];
    const size_t src_buf_size
        = src_concrete.offsets[position] - src_concrete.offsets[position - 1]; /// -1th index is Ok, see PaddedPODArray.

    const size_t old_size = chars.size();
    const size_t new_size = old_size + src_buf_size * length;
    chars.resize(new_size);

    const size_t old_rows = offsets.size();
    offsets.resize(old_rows + length);

    for (size_t current_offset = old_size; current_offset < new_size; current_offset += src_buf_size)
        memcpySmallAllowReadWriteOverflow15(&chars[current_offset], src_buf, src_buf_size);

    for (size_t i = 0, current_offset = old_size + src_buf_size; i < length; ++i, current_offset += src_buf_size)
        offsets[old_rows + i] = current_offset;
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
        res->offsets.resize_exact(to_size);

        for (size_t i = from_size; i < to_size; ++i)
        {
            ++offset;
            res->offsets[i] = offset;
        }
    }

    return res;
}

WeakHash32 ColumnString::getWeakHash32() const
{
    auto s = offsets.size();
    WeakHash32 hash(s);

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

    return hash;
}


#if !defined(DEBUG_OR_SANITIZER_BUILD)
void ColumnString::insertRangeFrom(const IColumn & src, size_t start, size_t length)
#else
void ColumnString::doInsertRangeFrom(const IColumn & src, size_t start, size_t length)
#endif
{
    if (length == 0)
        return;

    const ColumnString & src_concrete = assert_cast<const ColumnString &>(src);

    if (start + length > src_concrete.offsets.size())
        throw Exception(ErrorCodes::PARAMETER_OUT_OF_BOUND, "Parameter out of bound in IColumnString::insertRangeFrom method.");

    size_t nested_offset = src_concrete.offsetAt(start);
    size_t nested_length = src_concrete.offsets[start + length - 1] - nested_offset;

    /// Reserve offsets before to make it more exception safe (in case of MEMORY_LIMIT_EXCEEDED)
    offsets.reserve(offsets.size() + length);

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
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Mask size should be no less than data size.");

    /// We cannot change only offsets, because each string should end with terminating zero byte.
    /// So, we will insert one zero byte when mask value is zero.

    ssize_t index = mask.size() - 1;
    ssize_t from = offsets_data.size() - 1;
    /// mask.size() - offsets_data.size() should be equal to the number of zeros in mask
    /// (if not, one of exceptions below will throw) and we can calculate the resulting chars size.
    UInt64 last_offset = offsets_data[from] + (mask.size() - offsets_data.size());
    offsets_data.resize(mask.size());
    chars_data.resize_fill(last_offset);
    while (index >= 0)
    {
        offsets_data[index] = last_offset;
        if (!!mask[index] ^ inverted)
        {
            if (from < 0)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Too many bytes in mask");

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
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Not enough bytes in mask");
}


ColumnPtr ColumnString::permute(const Permutation & perm, size_t limit) const
{
    return permuteImpl(*this, perm, limit);
}

ColumnCheckpointPtr ColumnString::getCheckpoint() const
{
    auto nested = std::make_shared<ColumnCheckpoint>(chars.size());
    return std::make_shared<ColumnCheckpointWithNested>(size(), std::move(nested));
}

void ColumnString::updateCheckpoint(ColumnCheckpoint & checkpoint) const
{
    checkpoint.size = size();
    assert_cast<ColumnCheckpointWithNested &>(checkpoint).nested->size = chars.size();
}

void ColumnString::rollback(const ColumnCheckpoint & checkpoint)
{
    offsets.resize_assume_reserved(checkpoint.size);
    chars.resize_assume_reserved(assert_cast<const ColumnCheckpointWithNested &>(checkpoint).nested->size);
}

void ColumnString::collectSerializedValueSizes(PaddedPODArray<UInt64> & sizes, const UInt8 * is_null) const
{
    if (empty())
        return;

    size_t rows = size();
    if (sizes.empty())
        sizes.resize_fill(rows);
    else if (sizes.size() != rows)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Size of sizes: {} doesn't match rows_num: {}. It is a bug", sizes.size(), rows);

    if (is_null)
    {
        for (size_t i = 0; i < rows; ++i)
        {
            if (is_null[i])
            {
                ++sizes[i];
            }
            else
            {
                size_t string_size = sizeAt(i);
                sizes[i] += sizeof(string_size) + string_size + 1 /* null byte */;
            }
        }
    }
    else
    {
        for (size_t i = 0; i < rows; ++i)
        {
            size_t string_size = sizeAt(i);
            sizes[i] += sizeof(string_size) + string_size;
        }
    }
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

char * ColumnString::serializeValueIntoMemory(size_t n, char * memory) const
{
    size_t string_size = sizeAt(n);
    size_t offset = offsetAt(n);

    memcpy(memory, &string_size, sizeof(string_size));
    memory += sizeof(string_size);
    memcpy(memory, &chars[offset], string_size);
    return memory + string_size;
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

struct ColumnString::ComparatorBase
{
    const ColumnString & parent;

    explicit ComparatorBase(const ColumnString & parent_)
        : parent(parent_)
    {
    }

    ALWAYS_INLINE int compare(size_t lhs, size_t rhs) const
    {
        int res = memcmpSmallAllowOverflow15(
            parent.chars.data() + parent.offsetAt(lhs), parent.sizeAt(lhs) - 1,
            parent.chars.data() + parent.offsetAt(rhs), parent.sizeAt(rhs) - 1);

        return res;
    }
};

struct ColumnString::ComparatorCollationBase
{
    const ColumnString & parent;
    const Collator * collator;

    explicit ComparatorCollationBase(const ColumnString & parent_, const Collator * collator_)
        : parent(parent_), collator(collator_)
    {
    }

    ALWAYS_INLINE int compare(size_t lhs, size_t rhs) const
    {
        int res = collator->compare(
            reinterpret_cast<const char *>(&parent.chars[parent.offsetAt(lhs)]), parent.sizeAt(lhs),
            reinterpret_cast<const char *>(&parent.chars[parent.offsetAt(rhs)]), parent.sizeAt(rhs));

        return res;
    }
};

void ColumnString::getPermutation(PermutationSortDirection direction, PermutationSortStability stability,
                                size_t limit, int /*nan_direction_hint*/, Permutation & res) const
{
    if (direction == IColumn::PermutationSortDirection::Ascending && stability == IColumn::PermutationSortStability::Unstable)
        getPermutationImpl(limit, res, ComparatorAscendingUnstable(*this), DefaultSort(), DefaultPartialSort());
    else if (direction == IColumn::PermutationSortDirection::Ascending && stability == IColumn::PermutationSortStability::Stable)
        getPermutationImpl(limit, res, ComparatorAscendingStable(*this), DefaultSort(), DefaultPartialSort());
    else if (direction == IColumn::PermutationSortDirection::Descending && stability == IColumn::PermutationSortStability::Unstable)
        getPermutationImpl(limit, res, ComparatorDescendingUnstable(*this), DefaultSort(), DefaultPartialSort());
    else if (direction == IColumn::PermutationSortDirection::Descending && stability == IColumn::PermutationSortStability::Stable)
        getPermutationImpl(limit, res, ComparatorDescendingStable(*this), DefaultSort(), DefaultPartialSort());
}

void ColumnString::updatePermutation(PermutationSortDirection direction, PermutationSortStability stability,
                                size_t limit, int /*nan_direction_hint*/, Permutation & res, EqualRanges & equal_ranges) const
{
    auto comparator_equal = ComparatorEqual(*this);

    if (direction == IColumn::PermutationSortDirection::Ascending && stability == IColumn::PermutationSortStability::Unstable)
        updatePermutationImpl(limit, res, equal_ranges, ComparatorAscendingUnstable(*this), comparator_equal, DefaultSort(), DefaultPartialSort());
    else if (direction == IColumn::PermutationSortDirection::Ascending && stability == IColumn::PermutationSortStability::Stable)
        updatePermutationImpl(limit, res, equal_ranges, ComparatorAscendingStable(*this), comparator_equal, DefaultSort(), DefaultPartialSort());
    else if (direction == IColumn::PermutationSortDirection::Descending && stability == IColumn::PermutationSortStability::Unstable)
        updatePermutationImpl(limit, res, equal_ranges, ComparatorDescendingUnstable(*this), comparator_equal, DefaultSort(), DefaultPartialSort());
    else if (direction == IColumn::PermutationSortDirection::Descending && stability == IColumn::PermutationSortStability::Stable)
        updatePermutationImpl(limit, res, equal_ranges, ComparatorDescendingStable(*this), comparator_equal, DefaultSort(), DefaultPartialSort());
}

void ColumnString::getPermutationWithCollation(const Collator & collator, PermutationSortDirection direction, PermutationSortStability stability,
                                size_t limit, int /*nan_direction_hint*/, Permutation & res) const
{
    if (direction == IColumn::PermutationSortDirection::Ascending && stability == IColumn::PermutationSortStability::Unstable)
        getPermutationImpl(limit, res, ComparatorCollationAscendingUnstable(*this, &collator), DefaultSort(), DefaultPartialSort());
    else if (direction == IColumn::PermutationSortDirection::Ascending && stability == IColumn::PermutationSortStability::Stable)
        getPermutationImpl(limit, res, ComparatorCollationAscendingStable(*this, &collator), DefaultSort(), DefaultPartialSort());
    else if (direction == IColumn::PermutationSortDirection::Descending && stability == IColumn::PermutationSortStability::Unstable)
        getPermutationImpl(limit, res, ComparatorCollationDescendingUnstable(*this, &collator), DefaultSort(), DefaultPartialSort());
    else if (direction == IColumn::PermutationSortDirection::Descending && stability == IColumn::PermutationSortStability::Stable)
        getPermutationImpl(limit, res, ComparatorCollationDescendingStable(*this, &collator), DefaultSort(), DefaultPartialSort());
}

void ColumnString::updatePermutationWithCollation(const Collator & collator, PermutationSortDirection direction, PermutationSortStability stability,
                                size_t limit, int /*nan_direction_hint*/, Permutation & res, EqualRanges & equal_ranges) const
{
    auto comparator_equal = ComparatorCollationEqual(*this, &collator);

    if (direction == IColumn::PermutationSortDirection::Ascending && stability == IColumn::PermutationSortStability::Unstable)
        updatePermutationImpl(
            limit,
            res,
            equal_ranges,
            ComparatorCollationAscendingUnstable(*this, &collator),
            comparator_equal,
            DefaultSort(),
            DefaultPartialSort());
    else if (direction == IColumn::PermutationSortDirection::Ascending && stability == IColumn::PermutationSortStability::Stable)
        updatePermutationImpl(
            limit,
            res,
            equal_ranges,
            ComparatorCollationAscendingStable(*this, &collator),
            comparator_equal,
            DefaultSort(),
            DefaultPartialSort());
    else if (direction == IColumn::PermutationSortDirection::Descending && stability == IColumn::PermutationSortStability::Unstable)
        updatePermutationImpl(
            limit,
            res,
            equal_ranges,
            ComparatorCollationDescendingUnstable(*this, &collator),
            comparator_equal,
            DefaultSort(),
            DefaultPartialSort());
    else if (direction == IColumn::PermutationSortDirection::Descending && stability == IColumn::PermutationSortStability::Stable)
        updatePermutationImpl(
            limit,
            res,
            equal_ranges,
            ComparatorCollationDescendingStable(*this, &collator),
            comparator_equal,
            DefaultSort(),
            DefaultPartialSort());
}

size_t ColumnString::estimateCardinalityInPermutedRange(const Permutation & permutation, const EqualRange & equal_range) const
{
    const size_t range_size = equal_range.size();
    if (range_size <= 1)
        return range_size;

    /// TODO use sampling if the range is too large (e.g. 16k elements, but configurable)
    StringHashSet elements;
    bool inserted = false;
    for (size_t i = equal_range.from; i < equal_range.to; ++i)
    {
        size_t permuted_i = permutation[i];
        StringRef value = getDataAt(permuted_i);
        elements.emplace(value, inserted);
    }
    return elements.size();
}

ColumnPtr ColumnString::replicate(const Offsets & replicate_offsets) const
{
    size_t col_size = size();
    if (col_size != replicate_offsets.size())
        throw Exception(ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH, "Size of offsets doesn't match size of column.");

    auto res = ColumnString::create();

    if (0 == col_size)
        return res;

    Offsets & res_offsets = res->offsets;
    res_offsets.resize_exact(replicate_offsets.back());

    Chars & res_chars = res->chars;
    size_t res_chars_size = 0;
    for (size_t i = 0; i < col_size; ++i)
    {
        size_t size_to_replicate = replicate_offsets[i] - replicate_offsets[i - 1];
        size_t string_size = offsets[i] - offsets[i - 1];
        res_chars_size += size_to_replicate * string_size;
    }
    res_chars.resize_exact(res_chars_size);

    size_t curr_row = 0;
    size_t curr_offset = 0;
    for (size_t i = 0; i < col_size; ++i)
    {
        const size_t size_to_replicate = replicate_offsets[i] - replicate_offsets[i - 1];
        const size_t string_size = offsets[i] - offsets[i-1];
        const UInt8 * src = &chars[offsets[i - 1]];
        for (size_t j = 0; j < size_to_replicate; ++j)
        {
            memcpySmallAllowReadWriteOverflow15(
                &res_chars[curr_offset], src, string_size);

            curr_offset += string_size;
            res_offsets[curr_row] = curr_offset;
            ++curr_row;
        }
    }

    return res;
}

void ColumnString::reserve(size_t n)
{
    offsets.reserve_exact(n);
}

size_t ColumnString::capacity() const
{
    return offsets.capacity();
}

void ColumnString::prepareForSquashing(const Columns & source_columns)
{
    size_t new_size = size();
    size_t new_chars_size = chars.size();
    for (const auto & source_column : source_columns)
    {
        const auto & source_string_column = assert_cast<const ColumnString &>(*source_column);
        new_size += source_string_column.size();
        new_chars_size += source_string_column.chars.size();
    }

    offsets.reserve_exact(new_size);
    chars.reserve_exact(new_chars_size);
}

void ColumnString::shrinkToFit()
{
    chars.shrink_to_fit();
    offsets.shrink_to_fit();
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

    ComparatorBase cmp_op(*this);

    for (size_t i = 1; i < col_size; ++i)
    {
        if (cmp_op.compare(i, min_idx) < 0)
            min_idx = i;
        else if (cmp_op.compare(max_idx, i) < 0)
            max_idx = i;
    }

    get(min_idx, min);
    get(max_idx, max);
}

ColumnPtr ColumnString::compress(bool force_compression) const
{
    const size_t source_chars_size = chars.size();
    const size_t source_offsets_elements = offsets.size();
    const size_t source_offsets_size = source_offsets_elements * sizeof(Offset);

    /// Don't compress small blocks.
    if (source_chars_size < 4096) /// A wild guess.
        return ColumnCompressed::wrap(this->getPtr());

    auto chars_compressed = ColumnCompressed::compressBuffer(chars.data(), source_chars_size, force_compression);

    /// Return original column if not compressible.
    if (!chars_compressed)
        return ColumnCompressed::wrap(this->getPtr());

    auto offsets_compressed = ColumnCompressed::compressBuffer(offsets.data(), source_offsets_size, /*force_compression=*/true);

    const size_t chars_compressed_size = chars_compressed->size();
    const size_t offsets_compressed_size = offsets_compressed->size();
    return ColumnCompressed::create(source_offsets_elements, chars_compressed_size + offsets_compressed_size,
        [
            my_chars_compressed = std::move(chars_compressed),
            my_offsets_compressed = std::move(offsets_compressed),
            source_chars_size,
            source_offsets_elements
        ]
        {
            auto res = ColumnString::create();

            res->getChars().resize(source_chars_size);
            res->getOffsets().resize(source_offsets_elements);

            ColumnCompressed::decompressBuffer(
                my_chars_compressed->data(), res->getChars().data(), my_chars_compressed->size(), source_chars_size);

            ColumnCompressed::decompressBuffer(
                my_offsets_compressed->data(), res->getOffsets().data(), my_offsets_compressed->size(), source_offsets_elements * sizeof(Offset));

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
    Offset last_offset = offsets.empty() ? 0 : offsets.back();
    if (last_offset != chars.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "ColumnString validation failed: size mismatch (internal logical error) {} != {}",
                        last_offset, chars.size());
}

}
