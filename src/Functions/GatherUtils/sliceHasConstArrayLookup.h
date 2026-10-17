#pragma once

#include <DataTypes/NumberTraits.h>
#include <Functions/GatherUtils/Sources.h>
#include <Functions/GatherUtils/sliceHasImplAnyAll.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/HashSet.h>
#include <Common/NaNUtils.h>

#include <algorithm>
#include <array>
#include <bit>
#include <bitset>
#include <limits>
#include <optional>

/// `hasAny` and `hasAll` when one argument is a constant array of numbers. The elements of a row are looked up in a table of the
/// constant array, so that a row costs a lookup per element instead of a search of the constant array for each element.
///
/// The table is built for every block. The rows of a block are first searched as in `sliceHasImplAnyAll`, and the table is built
/// only when the search has compared about as many elements as building the table costs. So a block where the search settles each
/// row with a few comparisons does not pay for the table, and any other block takes at most about twice as long as the faster way.
///
/// The results are the same as in `sliceHasImplAnyAll`: a NULL element matches only a NULL, and values are compared with
/// `accurate::equalsOp`, as both arguments are cast to their common type before.
namespace DB::GatherUtils::ConstArrayLookup
{

/// Elements are looked up by their bytes: two values of the same type are equal if and only if their bytes are, except for floats.
template <typename T>
using Key = typename NumberTraits::Construct<false, false, sizeof(T)>::Type;

/// Returns false for NaN, which is not equal to anything. Zeros of both signs are equal, so they get the same key.
template <typename T>
ALWAYS_INLINE bool makeKey(T value, Key<T> & key)
{
    if constexpr (is_floating_point<T>)
    {
        if (isNaN(value))
            return false;
        if (value == T(0))
            value = T(0);
    }
    key = std::bit_cast<Key<T>>(value);
    return true;
}

/// `sliceContains` searches integers with vectors; other types are compared one by one.
template <typename T>
constexpr bool is_vector_searched = std::is_integral_v<T>;

/// Values of up to 2 bytes are looked up in a bitmap, which is faster than a search of any but the shortest constant arrays.
template <typename T>
constexpr bool uses_bitmap = sizeof(T) <= 2;

enum class Operation : uint8_t
{
    /// `hasAny` with either argument constant, and `hasAll(constant, column)`: each element of a row is searched in the constant.
    SearchElementsInConst,
    /// `hasAny(column, constant)`: each element of the constant is searched in a row, until one is found.
    SearchConstInRows,
    /// `hasAll(column, constant)`: each element of the constant is searched in a row, until one is not found.
    CountConstInRows,
};

/// The constant array must have this many elements for the lookup to be faster than the search in `sliceHasImplAnyAll`. Measured on
/// x86 with AVX2, with rows of 5 to 350 elements on average and with values that are found and that are not:
/// - Integers are searched with vectors, 128 bytes at once, so the lookup pays off from a number of bytes of the constant.
/// - `hasAll(column, constant)` finds each value of the constant on average in the middle of a row, and stops at the first value that
///   is not found, while the lookup goes through the row until all values are found. So it needs the longest constant.
/// - Other types are compared one by one, and the lookup pays off early, but a 32-byte value takes longer to hash.
/// - A bitmap of values of 1 or 2 bytes is faster than a search of 16 values.
template <typename T>
constexpr size_t minConstSize(Operation operation)
{
    if (operation == Operation::CountConstInRows)
    {
        if constexpr (is_vector_searched<T>)
            return 1024 / sizeof(T);
        else
            return sizeof(T) > 16 ? 128 : 64;
    }
    if constexpr (uses_bitmap<T>)
        return 16;
    else if constexpr (!is_vector_searched<T>)
        return sizeof(T) > 16 ? 32 : 16;
    else if (operation == Operation::SearchElementsInConst)
        return 768 / sizeof(T);
    else
        return 384 / sizeof(T);
}

/// Building the table costs about as much as comparing this many elements in a search, per element of the constant array.
template <typename T>
constexpr size_t search_work_per_const_element = is_vector_searched<T> ? 64 : 8;

/// Lookups mostly miss, and a miss probes until an empty cell, so a small hash table is kept at most 1/8 full: at 1/4 to 1/2, a
/// miss took twice as long because of mispredicted branches. A large table is kept at most 1/2 full: it does not fit in the cache
/// either way, and is built for every block and every thread.
template <typename Table>
size_t hashTableReserve(size_t size)
{
    static constexpr size_t max_sparse_table_bytes = 1 << 20;
    return 16 * size * sizeof(typename Table::cell_type) <= max_sparse_table_bytes ? 4 * size : size;
}

/// The elements of an array and their null map, which is `nullptr` if the array is not `Nullable`.
template <typename T>
struct Elements
{
    const T * data;
    const UInt8 * null_map;
    size_t size;
};

template <bool has_null_map, typename T>
ALWAYS_INLINE bool contains(const T * data, const UInt8 * null_map, size_t size, T value)
{
    if constexpr (is_vector_searched<T>)
    {
        return sliceContains<has_null_map>(NumericArraySlice<T>{data, size}, null_map, value);
    }
    else
    {
        /// `==` is `accurate::equalsOp` for values of the same type.
        for (size_t i = 0; i < size; ++i)
            if ((!has_null_map || !null_map[i]) && data[i] == value)
                return true;
        return false;
    }
}

/// Searches a value in the constant array, and adds the number of compared elements to `work`.
template <typename T>
ALWAYS_INLINE bool constContains(const Elements<T> & array, T value, size_t & work)
{
    /// The array is searched in parts that grow from the size of a few vectors, so that a value found early is counted as about
    /// the work that the search did.
    size_t part_size = 32;
    for (size_t begin = 0; begin < array.size; begin += part_size, part_size = std::min(2 * part_size, size_t(1024)))
    {
        const size_t size = std::min(part_size, array.size - begin);
        work += size;
        const bool found = array.null_map ? contains<true>(array.data + begin, array.null_map + begin, size, value)
                                          : contains<false>(array.data + begin, nullptr, size, value);
        if (found)
            return true;
    }
    return false;
}

/// The set of the non-NULL values of the constant array.
template <typename T>
class ValueSet
{
public:
    explicit ValueSet(const Elements<T> & array)
        : table(makeTable(array.size))
    {
        for (size_t i = 0; i < array.size; ++i)
        {
            Key<T> key{};
            if (!(array.null_map && array.null_map[i]) && makeKey(array.data[i], key))
            {
                if constexpr (uses_bitmap<T>)
                    table.set(key);
                else
                    table.insert(key);
            }
        }
    }

    ALWAYS_INLINE bool has(T value) const
    {
        Key<T> key{};
        if (!makeKey(value, key))
            return false;
        if constexpr (uses_bitmap<T>)
            return table.test(key);
        else
            return table.has(key);
    }

private:
    static constexpr size_t bitmap_size = uses_bitmap<T> ? 1 << (8 * sizeof(T)) : 1;
    using Table = std::conditional_t<uses_bitmap<T>, std::bitset<bitmap_size>, HashSet<Key<T>>>;

    static Table makeTable(size_t size)
    {
        if constexpr (uses_bitmap<T>)
            return {};
        else
            return Table(hashTableReserve<Table>(size));
    }

    Table table;
};

/// For each counted value, the number of the last row that contained it, plus one, so that zero means none.
template <typename T>
class LastRows
{
public:
    explicit LastRows(size_t size)
        : table(makeTable(size))
    {
    }

    ALWAYS_INLINE void insert(Key<T> key)
    {
        if constexpr (sizeof(T) == 1)
        {
            count += table[key] == not_counted;
            table[key] = 0;
        }
        else
        {
            typename Table::LookupResult it;
            bool inserted = false;
            table.emplace(key, it, inserted);
            if (inserted)
            {
                it->getMapped() = 0;
                ++count;
            }
        }
    }

    /// Returns whether the value is counted and was not marked for the row yet, and marks it.
    ALWAYS_INLINE bool mark(T value, UInt32 row_mark)
    {
        Key<T> key{};
        if (!makeKey(value, key))
            return false;
        UInt32 * last_row = nullptr;
        if constexpr (sizeof(T) == 1)
        {
            last_row = &table[key];
            if (*last_row == not_counted)
                return false;
        }
        else
        {
            auto * it = table.find(key);
            if (!it)
                return false;
            last_row = &it->getMapped();
        }
        if (*last_row == row_mark)
            return false;
        *last_row = row_mark;
        return true;
    }

    /// The number of counted values.
    size_t size() const { return count; }

private:
    static constexpr UInt32 not_counted = std::numeric_limits<UInt32>::max();
    using Table = std::conditional_t<sizeof(T) == 1, std::array<UInt32, 256>, HashMap<Key<T>, UInt32>>;

    static Table makeTable(size_t size)
    {
        if constexpr (sizeof(T) == 1)
        {
            Table table;
            table.fill(not_counted);
            return table;
        }
        else
            return Table(hashTableReserve<Table>(size));
    }

    Table table;
    size_t count = 0;
};

/// Calls `search_row` for the rows until it has done `search_work` work, then `lookup_rows` for the rest. `search_row` returns false
/// without a result when the work is done in the middle of the row, so that a long row does not take much longer than the work.
template <typename SearchRow, typename LookupRows>
void searchThenLookup(const ColumnArray::Offsets & offsets, size_t size, size_t search_work, SearchRow && search_row, LookupRows && lookup_rows)
{
    size_t work = 0;
    size_t row = 0;
    while (row < size && search_row(row, offsets[row - 1], offsets[row], work, search_work))
        ++row;
    if (row < size)
        lookup_rows(row);
}

/// `hasAny` with either argument constant, and `hasAll(constant, column)`. The search is the one of `sliceHasImplAnyAll`, which
/// searches each element of the second argument in the first.
template <ArraySearchType search_type, bool has_null_map, typename T>
void lookupElements(
    const Elements<T> & array, bool const_is_first, const Elements<T> & elements, const ColumnArray::Offsets & offsets,
    UInt8 * result, size_t size)
{
    static constexpr bool all = search_type == ArraySearchType::All;
    const bool const_has_null = hasNull(array.null_map, array.size);

    /// As in `sliceHasImplAnyAll`, a NULL of the row settles the result before its values if the constant has a NULL for `hasAny`, or
    /// has none for `hasAll`. Otherwise NULLs do not change the result.
    const bool null_settles = const_has_null != all;
    auto settled_by_null = [&](size_t begin, size_t end) ALWAYS_INLINE
    {
        return has_null_map && null_settles && hasNull(elements.null_map + begin, end - begin);
    };

    /// Whether the element settles the result of the row: it is in the constant for `hasAny`, it is not for `hasAll`.
    auto settles = [&](size_t i, auto && is_in_const) ALWAYS_INLINE
    {
        return !(has_null_map && elements.null_map[i]) && is_in_const(elements.data[i]) != all;
    };

    auto search_row = [&](size_t row, size_t begin, size_t end, size_t & work, size_t search_work) ALWAYS_INLINE
    {
        /// The constant is not empty, so only `hasAll(constant, column)` is true for an empty row.
        if (begin == end)
        {
            result[row] = all;
            return true;
        }
        bool settled = settled_by_null(begin, end);
        if (const_is_first)
        {
            for (size_t i = begin; i < end && !settled; ++i)
            {
                if (work >= search_work)
                    return false;
                settled = settles(i, [&](T value) ALWAYS_INLINE { return constContains(array, value, work); });
            }
        }
        else
        {
            /// `hasAny(column, constant)` searches each element of the constant in the row.
            const size_t row_size = end - begin;
            const UInt8 * null_map = has_null_map ? elements.null_map + begin : nullptr;
            for (size_t j = 0; j < array.size && !settled; ++j)
            {
                if (array.null_map && array.null_map[j])
                    continue;
                if (work >= search_work)
                    return false;
                work += row_size + 1;
                settled = contains<has_null_map>(elements.data + begin, null_map, row_size, array.data[j]);
            }
        }
        result[row] = settled != all;
        return true;
    };

    auto lookup_rows = [&](size_t from_row)
    {
        const ValueSet<T> set(array);
        for (size_t row = from_row; row < size; ++row)
        {
            bool settled = settled_by_null(offsets[row - 1], offsets[row]);
            for (size_t i = offsets[row - 1]; i < offsets[row] && !settled; ++i)
                settled = settles(i, [&](T value) ALWAYS_INLINE { return set.has(value); });
            result[row] = settled != all;
        }
    };

    searchThenLookup(offsets, size, search_work_per_const_element<T> * array.size, search_row, lookup_rows);
}

/// The first values are searched as in `sliceHasImplAnyAll` before the lookup. A missing value is usually one of them, and the search
/// finds that faster than a lookup of every element of the row. The lookup is then used only for the rows where the search of all
/// values would take longer.
constexpr size_t num_searched_values = 4;

/// `hasAll(column, constant)`: every element of the constant array is in the row. A row counts the distinct values it contains,
/// marking each one with the number of the row to count it once.
template <bool has_null_map, typename T>
void lookupAllOfConst(
    const Elements<T> & array, const Elements<T> & elements, const ColumnArray::Offsets & offsets, UInt8 * result, size_t size)
{
    const bool needs_null = hasNull(array.null_map, array.size);
    /// A NULL is contained only in a `Nullable` array.
    if (needs_null && !has_null_map)
    {
        std::fill(result, result + size, 0);
        return;
    }

    /// Whether the row contains the non-NULL values, and a NULL if the constant has one. Returns `std::nullopt` if `can_search`
    /// returns false before a value is searched.
    auto has_all_values_of = [&](size_t begin, size_t end, const T * values, const UInt8 * values_null_map, size_t num_values,
                                 auto && can_search) ALWAYS_INLINE -> std::optional<bool>
    {
        const UInt8 * null_map = has_null_map ? elements.null_map + begin : nullptr;
        if (needs_null && !hasNull(null_map, end - begin))
            return false;
        for (size_t j = 0; j < num_values; ++j)
        {
            if (values_null_map && values_null_map[j])
                continue;
            if (!can_search())
                return std::nullopt;
            if (!contains<has_null_map>(elements.data + begin, null_map, end - begin, values[j]))
                return false;
        }
        return true;
    };

    auto search_row = [&](size_t row, size_t begin, size_t end, size_t & work, size_t search_work) ALWAYS_INLINE
    {
        /// The constant is not empty, and a row must contain a NULL for each NULL of it.
        if (begin == end)
        {
            result[row] = 0;
            return true;
        }
        const auto all = has_all_values_of(begin, end, array.data, array.null_map, array.size, [&]() ALWAYS_INLINE
        {
            if (work >= search_work)
                return false;
            work += end - begin + 1;
            return true;
        });
        if (!all)
            return false;
        result[row] = *all;
        return true;
    };

    auto lookup_rows = [&](size_t from_row)
    {
        /// The first distinct values are searched, the others are counted.
        std::array<T, num_searched_values> searched_values{};
        std::array<Key<T>, num_searched_values> searched_keys{};
        size_t num_searched = 0;
        LastRows<T> last_rows(array.size);
        for (size_t i = 0; i < array.size; ++i)
        {
            Key<T> key{};
            if (array.null_map && array.null_map[i])
                continue;
            if (!makeKey(array.data[i], key))
            {
                /// NaN is not contained in any array.
                std::fill(result + from_row, result + size, 0);
                return;
            }
            if (std::find(searched_keys.begin(), searched_keys.begin() + num_searched, key) != searched_keys.begin() + num_searched)
                continue;
            if (num_searched < num_searched_values)
            {
                searched_keys[num_searched] = key;
                searched_values[num_searched++] = array.data[i];
            }
            else
                last_rows.insert(key);
        }

        const size_t num_counted = last_rows.size();
        /// A row with fewer elements than distinct values cannot contain them all.
        const size_t min_row_size = num_searched + num_counted;
        for (size_t row = from_row; row < size; ++row)
        {
            const size_t begin = offsets[row - 1];
            const size_t end = offsets[row];
            bool all = end - begin >= min_row_size && *has_all_values_of(begin, end, searched_values.data(), nullptr, num_searched, [] { return true; });
            if (all && num_counted)
            {
                const auto row_mark = static_cast<UInt32>(row + 1);
                size_t found = 0;
                for (size_t i = begin; i < end && found < num_counted; ++i)
                    found += (!has_null_map || !elements.null_map[i]) && last_rows.mark(elements.data[i], row_mark);
                all = found == num_counted;
            }
            result[row] = all;
        }
    };

    searchThenLookup(offsets, size, search_work_per_const_element<T> * array.size, search_row, lookup_rows);
}

template <typename T>
Elements<T> getElements(const NumericArraySource<T> & source, bool is_nullable)
{
    const UInt8 * null_map = is_nullable ? static_cast<const NullableArraySource<NumericArraySource<T>> &>(source).null_map.data() : nullptr;
    return {source.elements.data(), null_map, source.elements.size()};
}

/// Executes `hasAny` or `hasAll` with the lookup if one argument is a constant array of numbers that is long enough. Returns false to
/// leave them to `sliceHasImplAnyAll`.
template <ArraySearchType search_type, typename T>
bool tryExecute(
    const NumericArraySource<T> & first, bool is_first_const, bool is_first_nullable,
    const NumericArraySource<T> & second, bool is_second_const, bool is_second_nullable,
    UInt8 * result, size_t size)
{
    if (is_first_const == is_second_const)
        return false;

    /// `sliceHasImplAnyAll` searches each element of `second` in `first`.
    const Operation operation = is_first_const ? Operation::SearchElementsInConst
        : (search_type == ArraySearchType::Any ? Operation::SearchConstInRows : Operation::CountConstInRows);
    /// The data column of a constant has one row. The rows are numbered with `UInt32` in `LastRows`.
    const Elements<T> array = is_first_const ? getElements(first, is_first_nullable) : getElements(second, is_second_nullable);
    if (array.size < minConstSize<T>(operation) || size >= std::numeric_limits<UInt32>::max())
        return false;

    const Elements<T> elements = is_first_const ? getElements(second, is_second_nullable) : getElements(first, is_first_nullable);
    const ColumnArray::Offsets & offsets = is_first_const ? second.offsets : first.offsets;

    auto execute = [&]<bool has_null_map>()
    {
        if (operation == Operation::CountConstInRows)
            lookupAllOfConst<has_null_map>(array, elements, offsets, result, size);
        else
            lookupElements<search_type, has_null_map>(array, is_first_const, elements, offsets, result, size);
    };
    if (elements.null_map)
        execute.template operator()<true>();
    else
        execute.template operator()<false>();
    return true;
}

template <ArraySearchType, typename Source>
bool tryExecute(const Source &, bool, bool, const Source &, bool, bool, UInt8 *, size_t)
{
    return false;
}

}
