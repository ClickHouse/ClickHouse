#include <Functions/GatherUtils/GatherUtils.h>
#include <Functions/GatherUtils/Selectors.h>
#include <Functions/GatherUtils/Algorithms.h>
#include <Columns/ColumnString.h>
#include <Common/HashTable/HashSet.h>

#include <bitset>

namespace DB::GatherUtils
{

namespace
{

constexpr size_t min_needles_for_lookup = 4;
/// Hashing a longer string can cost more than the comparisons that the set saves.
constexpr size_t max_needle_size = 256;

/// Looks every element up in a hash set of the needles. Returns false to leave the block to the generic path.
bool hasAnyStringInConstNeedles(const GenericArraySource & first, const GenericArraySource & second, UInt8 * result, size_t size)
{
    const auto * haystack = typeid_cast<const ColumnString *>(&first.elements);
    const auto * needle_column = typeid_cast<const ColumnString *>(&second.elements);
    if (!haystack || !needle_column)
        return false;

    const GenericArraySlice needles = second.getWhole();
    /// The generic path may settle a row in one comparison, so the set must pay off over enough rows.
    size_t min_rows = 4 * needles.size;
    if (needles.size < min_needles_for_lookup || min_rows > size)
        return false;

    for (size_t i = needles.begin; i < needles.begin + needles.size; ++i)
    {
        const size_t needle_size = needle_column->getDataAt(i).size();
        min_rows += needle_size / 8;
        if (needle_size > max_needle_size || min_rows > size)
            return false;
    }

    HashSet<std::string_view> needle_set;
    std::bitset<max_needle_size + 1> needle_lengths;
    for (size_t i = needles.begin; i < needles.begin + needles.size; ++i)
    {
        const std::string_view needle = needle_column->getDataAt(i);
        needle_lengths[needle.size()] = true;
        needle_set.insert(needle);
    }

    const auto & offsets = first.offsets;
    size_t prev_offset = 0;
    for (size_t row = 0; row < size; ++row)
    {
        UInt8 found = 0;
        for (size_t j = prev_offset; j < offsets[row]; ++j)
        {
            const std::string_view element = haystack->getDataAt(j);
            if (element.size() <= max_needle_size && needle_lengths[element.size()] && needle_set.has(element))
            {
                found = 1;
                break;
            }
        }
        result[row] = found;
        prev_offset = offsets[row];
    }
    return true;
}

struct ArrayHasAnySelectArraySourcePair : public ArraySourcePairSelector<ArrayHasAnySelectArraySourcePair>
{
    template <typename FirstSource, typename SecondSource>
    static void callFunction(FirstSource && first,
                             bool is_second_const, bool is_second_nullable, SecondSource && second,
                             UInt8 * result, size_t size)
    {
        using SourceType = typename std::decay_t<SecondSource>;

        if (is_second_nullable)
        {
            using NullableSource = NullableArraySource<SourceType>;

            if (is_second_const)
                arrayAllAny<ArraySearchType::Any>(first, static_cast<ConstSource<NullableSource> &>(second), result, size);
            else
                arrayAllAny<ArraySearchType::Any>(first, static_cast<NullableSource &>(second), result, size);
        }
        else
        {
            if (is_second_const)
            {
                if constexpr (std::is_same_v<std::decay_t<FirstSource>, GenericArraySource>
                    && std::is_same_v<SourceType, GenericArraySource>)
                {
                    if (hasAnyStringInConstNeedles(first, second, result, size))
                        return;
                }
                arrayAllAny<ArraySearchType::Any>(first, static_cast<ConstSource<SourceType> &>(second), result, size);
            }
            else
                arrayAllAny<ArraySearchType::Any>(first, second, result, size);
        }
    }

    template <typename Source>
    static void selectSourcePair(bool is_first_const, bool is_first_nullable, Source && first,
                                 bool is_second_const, bool is_second_nullable, Source && second,
                                 UInt8 * result, size_t size)
    {
        using SourceType = typename std::decay_t<Source>;

        if (is_first_nullable)
        {
            using NullableSource = NullableArraySource<SourceType>;

            if (is_first_const)
                callFunction(static_cast<ConstSource<NullableSource> &>(first), is_second_const, is_second_nullable, second, result, size);
            else
                callFunction(static_cast<NullableSource &>(first), is_second_const, is_second_nullable, second, result, size);
        }
        else
        {
            if (is_first_const)
                callFunction(static_cast<ConstSource<SourceType> &>(first), is_second_const, is_second_nullable, second, result, size);
            else
                callFunction(first, is_second_const, is_second_nullable, second, result, size);
        }
    }
};

}

void sliceHasAny(IArraySource & first, IArraySource & second, ColumnUInt8 & result)
{
    ArrayHasAnySelectArraySourcePair::select(first, second, result.getData().data(), result.size());
}

}
