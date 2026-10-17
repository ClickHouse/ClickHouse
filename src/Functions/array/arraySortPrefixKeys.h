#pragma once

#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/StringPrefixKey.h>


/// Getters of string prefix keys (see `StringPrefixKey.h`) for the functions that sort the elements of arrays
/// with their own comparators: `arraySort`, `arrayPartialSort`, `arrayTopK`, `mapSort` and similar.

namespace DB
{

/// Strings of a `Nullable` column, for comparators that put `NULL`s last in both directions:
/// `NULL`s get the last possible key, and the ties with it are resolved by the comparator.
/// The nested values of `NULL`s take part in the common prefix, which can only make it shorter.
template <bool positive, typename NestedGetString>
struct NullablePrefixKeyGetter
{
    NestedGetString nested;
    const UInt8 * null_map;

    ALWAYS_INLINE std::string_view get(size_t row) const { return nested.get(row); }

    ALWAYS_INLINE UInt64 key(size_t row, size_t offset) const
    {
        if (null_map[row])
            return positive ? std::numeric_limits<UInt64>::max() : 0;
        return nested.key(row, offset);
    }
};

template <bool positive, typename NestedGetString>
NullablePrefixKeyGetter<positive, NestedGetString> makeNullablePrefixKeyGetter(NestedGetString nested, const NullMap & null_map)
{
    return {nested, null_map.data()};
}

/// Strings of a `LowCardinality` column, taken from its dictionary.
template <typename DictionaryGetString, typename Index>
struct DictionaryPrefixKeyGetter
{
    DictionaryGetString dictionary;
    const Index * indexes;

    ALWAYS_INLINE std::string_view get(size_t row) const { return dictionary.get(indexes[row]); }
    ALWAYS_INLINE UInt64 key(size_t row, size_t offset) const { return dictionary.key(indexes[row], offset); }
};

/// Calls `apply(get_string)` with a getter of the strings of the rows of `column` (see `PrefixKeySort`)
/// if its values are strings, otherwise returns false.
template <typename Apply>
bool callWithStringPrefixKeyGetter(const IColumn & column, Apply && apply)
{
    if (const auto * column_string = checkAndGetColumn<ColumnString>(&column))
        apply(prefixKeyGetter(*column_string));
    else if (const auto * column_fixed_string = checkAndGetColumn<ColumnFixedString>(&column))
        apply(prefixKeyGetter(*column_fixed_string));
    else
        return false;
    return true;
}

/// Like `callWithStringPrefixKeyGetter`, but also for `LowCardinality` strings and for tuples whose first element
/// is a string, such as the key-value tuples of `mapSort`. The order of these values starts with the order of
/// the strings, so the prefix keys are consistent with it.
template <typename Apply>
bool callWithGenericStringPrefixKeyGetter(const IColumn & column, Apply && apply)
{
    if (const auto * tuple = checkAndGetColumn<ColumnTuple>(&column))
        return tuple->tupleSize() > 0 && callWithGenericStringPrefixKeyGetter(tuple->getColumn(0), apply);

    if (const auto * low_cardinality = checkAndGetColumn<ColumnLowCardinality>(&column))
    {
        if (low_cardinality->nestedIsNullable())
            return false;

        return callWithStringPrefixKeyGetter(*low_cardinality->getDictionary().getNestedColumn(), [&](auto dictionary_get_string)
        {
            auto apply_for_indexes = [&]<typename Index>()
            {
                const auto & indexes = assert_cast<const ColumnVector<Index> &>(low_cardinality->getIndexes());
                apply(DictionaryPrefixKeyGetter<decltype(dictionary_get_string), Index>{dictionary_get_string, indexes.getData().data()});
            };

            switch (low_cardinality->getSizeOfIndexType())
            {
                case sizeof(UInt8): apply_for_indexes.template operator()<UInt8>(); break;
                case sizeof(UInt16): apply_for_indexes.template operator()<UInt16>(); break;
                case sizeof(UInt32): apply_for_indexes.template operator()<UInt32>(); break;
                case sizeof(UInt64): apply_for_indexes.template operator()<UInt64>(); break;
                default: throwUnexpectedLowCardinalityIndexType(low_cardinality->getSizeOfIndexType());
            }
        });
    }

    return callWithStringPrefixKeyGetter(column, apply);
}

}
