#pragma once

#include <DataTypes/NumberTraits.h>
#include <Functions/GatherUtils/Sources.h>
#include <Functions/GatherUtils/sliceEqualElements.h>
#include <Functions/GatherUtils/sliceHasImplAnyAll.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/HashSet.h>
#include <Common/NaNUtils.h>

#include <algorithm>
#include <array>
#include <bit>
#include <bitset>
#include <limits>

/// `hasAny` and `hasAll` when one argument is a constant array of numbers. The elements of a row are looked up in a table of the
/// constant array, so that a row costs a lookup per element instead of a search of the constant array for each element.
///
/// The table is built for every block. The rows of a block are first searched as in `sliceHasImplAnyAll`, and the table is built
/// only when the search has taken about as long as building the table and looking up the searched rows would take. So
/// a block where the search settles each row with a few comparisons does not pay for the table, and a block that builds it takes at
/// most about twice as long as with the table from the start.
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
    /// `hasAny(constant, column)` and `hasAll(constant, column)`: each element of a row is searched in the constant.
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

/// The work of the search and of the table, in units of about 10 ps, measured on x86 with AVX2 with values that are not found:
/// - The search compares an element of 1 to 4 bytes with vectors in 1 unit, of 8 bytes in 3, and one by one in about 22. Each
///   searched part of an array takes another 40 units with vectors, as a call of `sliceContains`, or 10 one by one.
/// - A bitmap or an array is tested in about 24 units, a hash table is probed in about 200, or 600 for a 32-byte value.
/// - The table is built in about 200 to 300 units per element of the constant array, 800 for a 32-byte value, 85 for a bitmap.
template <typename T>
constexpr size_t compare_work = is_vector_searched<T> ? (sizeof(T) == 8 ? 3 : 1) : 22;
template <typename T>
constexpr size_t search_call_work = is_vector_searched<T> ? 40 : 10;
constexpr size_t bitmap_lookup_work = 24;
template <typename T>
constexpr size_t hash_table_lookup_work = sizeof(T) > 16 ? 600 : 200;
template <typename T>
constexpr size_t build_work_per_const_element = uses_bitmap<T> ? 85 : (sizeof(T) > 16 ? 800 : (sizeof(T) > 8 ? 320 : 220));

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
        /// The comparison of `sliceHasImplAnyAll`. The `==` of `Decimal` is not inlined.
        const NumericArraySlice<T> slice{data, size};
        const NumericArraySlice<T> needle{&value, 1};
        for (size_t i = 0; i < size; ++i)
            if ((!has_null_map || !null_map[i]) && sliceEqualElements(slice, needle, i, 0))
                return true;
        return false;
    }
}

/// Searches a value, and adds the work of the search to `work`. The elements are searched in parts that grow from `first_part_size`
/// to 1024, so that a value found early is counted as about the work that the search did.
template <bool has_null_map, typename T>
ALWAYS_INLINE bool countedContains(const T * data, const UInt8 * null_map, size_t size, T value, size_t first_part_size, size_t & work)
{
    size_t part_size = first_part_size;
    for (size_t begin = 0; begin < size; begin += part_size, part_size = std::min(2 * part_size, size_t(1024)))
    {
        const size_t part = std::min(part_size, size - begin);
        work += search_call_work<T> + part * compare_work<T>;
        if (contains<has_null_map>(data + begin, has_null_map ? null_map + begin : nullptr, part, value))
            return true;
    }
    return false;
}

/// The constant array is searched in parts from a block of 128 bytes of `sliceContains`, or from one element if it is compared one by
/// one, so that a value near its start is counted as faster than a lookup, as it is. A row is searched in larger parts, as a part is an
/// extra call for each searched value, and the search of a row is usually not stopped early by a value at its start.
template <typename T>
constexpr size_t const_first_part_size = is_vector_searched<T> ? 128 / sizeof(T) : 1;
constexpr size_t row_first_part_size = 256;

/// A value in the first vector of a row, or in its first 8 elements if it is compared one by one, is found faster than by a lookup.
template <typename T>
constexpr size_t head_size = is_vector_searched<T> ? 32 / sizeof(T) : 8;

/// So is a value in the first block of 128 bytes of the constant array, which `sliceContains` searches at once. So the search of this
/// head is not counted in the work, as counting it would take about as long as the search. A constant size lets the compiler drop the
/// checks of the size from the loop.
template <typename T>
constexpr size_t const_head_size
    = std::min(is_vector_searched<T> ? 128 / sizeof(T) : head_size<T>, minConstSize<T>(Operation::SearchElementsInConst));

/// The set of the non-NULL values of the constant array.
template <typename T>
class ValueSet
{
public:
    static constexpr size_t lookup_work = uses_bitmap<T> ? bitmap_lookup_work : hash_table_lookup_work<T>;

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

    [[nodiscard]] [[nodiscard]] ALWAYS_INLINE bool has(T value) const
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
    static constexpr size_t lookup_work = sizeof(T) == 1 ? bitmap_lookup_work : hash_table_lookup_work<T>;

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
    [[nodiscard]] [[nodiscard]] size_t size() const { return count; }

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

/// The work of the search of the rows so far, and an estimate of the work of building the table and looking up the elements of these
/// rows.
struct Work
{
    size_t search = 0;
    size_t table = 0;
    /// The search may take `table >> table_shift`. A search after the table is built is wasted if it does not settle the row, so it
    /// is given a part of the work of the lookup.
    size_t table_shift = 0;

    [[nodiscard]] [[nodiscard]] size_t allowed() const { return table >> table_shift; }
    /// The table would have been faster for the rows searched so far.
    [[nodiscard]] [[nodiscard]] bool exceeded() const { return search > allowed(); }
};

/// Returns the position of the first of the non-NULL values of the array in `[begin, end)` that is in the row, or `end`. The search
/// of `sliceHasImplAnyAll`, in its own function, as it is slower when it is inlined into the loops of the lookup.
template <bool has_null_map, typename T>
NO_INLINE size_t findValue(const T * data, const UInt8 * null_map, size_t size, const Elements<T> & array, size_t begin, size_t end)
{
    for (size_t j = begin; j < end; ++j)
        if (!(array.null_map && array.null_map[j]) && contains<has_null_map>(data, null_map, size, array.data[j]))
            return j;
    return end;
}

template <typename T>
ALWAYS_INLINE bool headContains(const Elements<T> & array, T value)
{
    return array.null_map ? contains<true>(array.data, array.null_map, const_head_size<T>, value)
                          : contains<false>(array.data, nullptr, const_head_size<T>, value);
}

/// Searches the elements of a row in the constant array, as `sliceHasImplAnyAll` does, until one settles the row: it is in the constant
/// for `hasAny`, it is not for `hasAll`. Returns false without `settled` when the work is exceeded first. In its own function, as the
/// search is slower when it is inlined into the loops of the lookup.
template <bool all, bool has_null_map, bool const_has_null_map, typename T>
NO_INLINE bool searchElementsInConst(
    const T * data, const UInt8 * null_map, size_t size, const Elements<T> & array, Work & work, bool & settled)
{
    /// The constant has at least `minConstSize` elements.
    static constexpr size_t head = const_head_size<T>;
    const UInt8 * rest_null_map = const_has_null_map ? array.null_map + head : nullptr;
    for (size_t i = 0; i < size; ++i)
    {
        if (has_null_map && null_map[i])
            continue;
        bool in_const = contains<const_has_null_map>(array.data, array.null_map, head, data[i]);
        if (!in_const)
        {
            if (work.exceeded())
                return false;
            work.table += ValueSet<T>::lookup_work;
            work.search += search_call_work<T> + head * compare_work<T>;
            in_const = countedContains<const_has_null_map>(
                array.data + head, rest_null_map, array.size - head, data[i], const_first_part_size<T>, work.search);
        }
        if (in_const != all)
        {
            settled = true;
            return true;
        }
    }
    settled = false;
    return true;
}

/// A row after the table is built is searched within a quarter of the work of its lookup. That settles the rows that the first values
/// of the constant settle, and takes a quarter longer for the others.
constexpr size_t search_after_build_table_shift = 2;

/// The searches of the rows after the table is built may take `1 / lookup_work_per_wasted_search_work` of the work of the lookups
/// more than they saved.
constexpr Int64 lookup_work_per_wasted_search_work = 32;

/// Calls `search_row` for the rows until the work is exceeded, then `lookup_rows` for the rest. `search_row` returns false without a
/// result when the work is exceeded in the middle of the row, so that a long row does not take much longer than the lookup.
///
/// A row after the table is built is still searched first, within a part of the work of its lookup, as a row can be settled by one
/// scan for the first value of the constant, or by values in the head of the constant. It is searched while the searches saved more
/// work than the ones that did not settle their row took, plus a fraction of the work of the lookups. `lookup_rows` calls
/// `try_search(row)`, which returns whether the search settled the row, and `add_lookup_work(work)` for each row that it looks up.
template <typename SearchRow, typename LookupRows>
void searchThenLookup(
    const ColumnArray::Offsets & offsets, size_t size, size_t build_work, SearchRow && search_row, LookupRows && lookup_rows)
{
    Work work{.table = build_work};
    size_t row = 0;
    while (row < size && search_row(row, offsets[row - 1], offsets[row], work))
        ++row;
    if (row == size)
        return;

    /// In units of `1 / lookup_work_per_wasted_search_work` of work.
    Int64 allowed_search_work = 0;
    auto try_search = [&](size_t searched_row) ALWAYS_INLINE
    {
        if (allowed_search_work < 0)
            return false;
        Work row_work{.table_shift = search_after_build_table_shift};
        const bool searched = search_row(searched_row, offsets[searched_row - 1], offsets[searched_row], row_work);
        const Int64 saved_work = (searched ? static_cast<Int64>(row_work.table) : 0) - static_cast<Int64>(row_work.search);
        allowed_search_work += lookup_work_per_wasted_search_work * saved_work;
        return searched;
    };
    auto add_lookup_work = [&](size_t lookup_work) ALWAYS_INLINE { allowed_search_work += lookup_work; };
    lookup_rows(row, try_search, add_lookup_work);
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

    auto * search_elements_in_const = array.null_map ? searchElementsInConst<all, has_null_map, true, T>
                                                     : searchElementsInConst<all, has_null_map, false, T>;

    auto search_row = [&](size_t row, size_t begin, size_t end, Work & work) ALWAYS_INLINE
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
            const UInt8 * null_map = has_null_map ? elements.null_map + begin : nullptr;
            if constexpr (all && sizeof(T) == 1)
            {
                /// As in `sliceHasImplAnyAll`, 16 elements of a long row are searched at once, which is faster than the bitmap.
                if (!settled && end - begin >= 16)
                {
                    const NumericArraySlice<T> const_slice{array.data, array.size};
                    const NumericArraySlice<T> row_slice{elements.data + begin, end - begin};
                    result[row] = array.null_map ? sliceHasAllBytes<true>(const_slice, row_slice, array.null_map, null_map)
                                                 : sliceHasAllBytes<false>(const_slice, row_slice, nullptr, null_map);
                    return true;
                }
            }
            /// The first element of `hasAny` is searched here, as it often settles the row.
            if (!all && !settled && !(has_null_map && null_map[0]))
                settled = headContains(array, elements.data[begin]);
            if (!settled && !search_elements_in_const(elements.data + begin, null_map, end - begin, array, work, settled))
                return false;
        }
        else if (!settled)
        {
            /// `hasAny(column, constant)` searches each element of the constant in the row.
            const size_t row_size = end - begin;
            const UInt8 * null_map = has_null_map ? elements.null_map + begin : nullptr;
            /// The lookup would go through the row until an element is in the constant.
            work.table += ValueSet<T>::lookup_work * row_size;
            if (row_size <= row_first_part_size)
            {
                const size_t value_work = search_call_work<T> + row_size * compare_work<T>;
                /// The first value often settles the row, and is searched here, as that is faster than the call: in a row of up to one
                /// block of `sliceContains`, and otherwise in the first vector of the row, which the call searches again.
                size_t first_value = 0;
                if (!(array.null_map && array.null_map[0]))
                {
                    if (work.exceeded())
                        return false;
                    if (row_size * sizeof(T) <= 128 || row_size <= head_size<T>)
                    {
                        first_value = 1;
                        work.search += value_work;
                        settled = contains<has_null_map>(elements.data + begin, null_map, row_size, array.data[0]);
                    }
                    else if (is_vector_searched<T>)
                        settled = contains<has_null_map>(elements.data + begin, null_map, head_size<T>, array.data[0]);
                }
                if (!settled)
                {
                    /// Each value is searched in one part, so the number of values searched before the work is exceeded is known.
                    /// It is usually all of them, which does not need a division.
                    if (work.exceeded())
                        return false;
                    const size_t allowed_work = work.allowed() - work.search;
                    const size_t end_value = (array.size - first_value) * value_work <= allowed_work
                        ? array.size
                        : std::min(array.size, first_value + allowed_work / value_work + 1);
                    const size_t found
                        = findValue<has_null_map>(elements.data + begin, null_map, row_size, array, first_value, end_value);
                    settled = found < end_value;
                    work.search += ((settled ? found + 1 : end_value) - first_value) * value_work;
                    if (!settled && end_value < array.size)
                        return false;
                }
            }
            else
            {
                for (size_t j = 0; j < array.size && !settled; ++j)
                {
                    if (array.null_map && array.null_map[j])
                        continue;
                    if (work.exceeded())
                        return false;
                    settled = countedContains<has_null_map>(
                        elements.data + begin, null_map, row_size, array.data[j], row_first_part_size, work.search);
                }
            }
        }
        result[row] = settled != all;
        return true;
    };

    auto lookup_rows = [&](size_t from_row, auto && try_search, auto && add_lookup_work)
    {
        const ValueSet<T> set(array);
        for (size_t row = from_row; row < size; ++row)
        {
            if (try_search(row))
                continue;
            const size_t begin = offsets[row - 1];
            size_t i = begin;
            bool settled = settled_by_null(begin, offsets[row]);
            for (; i < offsets[row] && !settled; ++i)
                settled = settles(i, [&](T value) ALWAYS_INLINE { return set.has(value); });
            add_lookup_work(ValueSet<T>::lookup_work * (i - begin));
            result[row] = settled != all;
        }
    };

    searchThenLookup(offsets, size, build_work_per_const_element<T> * array.size, search_row, lookup_rows);
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

    auto search_row = [&](size_t row, size_t begin, size_t end, Work & work) ALWAYS_INLINE
    {
        /// The constant is not empty, and a row must contain a NULL for each NULL of it.
        if (begin == end)
        {
            result[row] = 0;
            return true;
        }
        const size_t row_size = end - begin;
        const UInt8 * null_map = has_null_map ? elements.null_map + begin : nullptr;
        /// The lookup searches the first values too, and goes through the row if it has them all. It rejects a row with fewer elements
        /// than the constant has distinct values, but their number is not known before the table is built, so every row is counted as
        /// looked up. That counts at most the work of the search of the row, so the search is not slower than without the table. After
        /// the table is built, the rows that the lookup rejects are not searched.
        bool all = !needs_null || hasNull(null_map, row_size);
        size_t num_found = 0;
        for (size_t j = 0; j < array.size && all; ++j)
        {
            if (array.null_map && array.null_map[j])
                continue;
            if (work.exceeded())
                return false;
            const size_t search_before = work.search;
            all = countedContains<has_null_map>(elements.data + begin, null_map, row_size, array.data[j], row_first_part_size, work.search);
            if (num_found < num_searched_values)
            {
                /// The lookup searches this value too.
                work.search = search_before;
                if (all && ++num_found == num_searched_values)
                    work.table += LastRows<T>::lookup_work * row_size;
            }
        }
        result[row] = all;
        return true;
    };

    auto lookup_rows = [&](size_t from_row, auto && try_search, auto && add_lookup_work)
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
            const size_t row_size = end - begin;
            if (row_size < min_row_size)
            {
                result[row] = 0;
                continue;
            }
            if (try_search(row))
                continue;
            const UInt8 * null_map = has_null_map ? elements.null_map + begin : nullptr;
            bool all = (!needs_null || hasNull(null_map, row_size))
                && std::all_of(searched_values.begin(), searched_values.begin() + num_searched, [&](T value) ALWAYS_INLINE
                               { return contains<has_null_map>(elements.data + begin, null_map, row_size, value); });
            if (all && num_counted)
            {
                const auto row_mark = static_cast<UInt32>(row + 1);
                size_t found = 0;
                size_t i = begin;
                for (; i < end && found < num_counted; ++i)
                    found += (!has_null_map || !elements.null_map[i]) && last_rows.mark(elements.data[i], row_mark);
                /// The search of the first values is not counted: the search of the row does it too, and saves nothing on it.
                add_lookup_work(LastRows<T>::lookup_work * (i - begin));
                all = found == num_counted;
            }
            result[row] = all;
        }
    };

    searchThenLookup(offsets, size, build_work_per_const_element<T> * array.size, search_row, lookup_rows);
}

template <typename T>
Elements<T> getElements(const NumericArraySource<T> & source, bool is_nullable)
{
    const UInt8 * null_map
        = is_nullable ? static_cast<const NullableArraySource<NumericArraySource<T>> &>(source).null_map.data() : nullptr;
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
        /// `hasAny` does not compile `lookupAllOfConst`.
        if constexpr (search_type == ArraySearchType::All)
        {
            if (operation == Operation::CountConstInRows)
            {
                lookupAllOfConst<has_null_map>(array, elements, offsets, result, size);
                return;
            }
        }
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
