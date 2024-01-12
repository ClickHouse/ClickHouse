#include <AggregateFunctions/SingleValueData.h>
#include <Columns/ColumnString.h>
#include <Common/findExtreme.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace
{

std::unique_ptr<UInt8[]>
mergeIfAndNullFlags(const UInt8 * __restrict null_map, const UInt8 * __restrict if_flags, size_t row_begin, size_t row_end)
{
    auto final_flags = std::make_unique<UInt8[]>(row_end);
    for (size_t i = row_begin; i < row_end; ++i)
        final_flags[i] = (!null_map[i]) & !!if_flags[i];
    return final_flags;
}

}

std::optional<size_t> SingleValueDataBase::getSmallestIndex(const IColumn & column, size_t row_begin, size_t row_end)
{
    if (row_begin >= row_end)
        return std::nullopt;

    /// TODO: Introduce row_begin and row_end to getPermutation
    if (row_begin != 0 || row_end != column.size())
    {
        size_t index = row_begin;
        for (size_t i = index + 1; i < row_end; i++)
            if ((column.compareAt(i, index, column, nan_direction_hint) < 0))
                index = i;
        return {index};
    }
    else
    {
        constexpr IColumn::PermutationSortDirection direction = IColumn::PermutationSortDirection::Ascending;
        constexpr IColumn::PermutationSortStability stability = IColumn::PermutationSortStability::Unstable;
        IColumn::Permutation permutation;
        constexpr UInt64 limit = 1;
        column.getPermutation(direction, stability, limit, nan_direction_hint, permutation);
        return {permutation[0]};
    }
}

std::optional<size_t> SingleValueDataBase::getGreatestIndex(const IColumn & column, size_t row_begin, size_t row_end)
{
    if (row_begin >= row_end)
        return std::nullopt;

    /// TODO: Introduce row_begin and row_end to getPermutation
    if (row_begin != 0 || row_end != column.size())
    {
        size_t index = row_begin;
        for (size_t i = index + 1; i < row_end; i++)
            if ((column.compareAt(i, index, column, nan_direction_hint) > 0))
                index = i;
        return {index};
    }
    else
    {
        constexpr IColumn::PermutationSortDirection direction = IColumn::PermutationSortDirection::Descending;
        constexpr IColumn::PermutationSortStability stability = IColumn::PermutationSortStability::Unstable;
        IColumn::Permutation permutation;
        constexpr UInt64 limit = 1;
        column.getPermutation(direction, stability, limit, nan_direction_hint, permutation);
        return {permutation[0]};
    }
}

std::optional<size_t> SingleValueDataBase::getSmallestIndexNotNullIf(
    const IColumn & column, const UInt8 * __restrict null_map, const UInt8 * __restrict if_map, size_t row_begin, size_t row_end)
{
    size_t index = row_begin;
    while ((index < row_end) && ((if_map && if_map[index] == 0) || (null_map && null_map[index] != 0)))
        index++;
    if (index >= row_end)
        return std::nullopt;

    for (size_t i = index + 1; i < row_end; i++)
        if ((!if_map || if_map[i] != 0) && (!null_map || null_map[i] == 0) && (column.compareAt(i, index, column, nan_direction_hint) < 0))
            index = i;
    return {index};
}

std::optional<size_t> SingleValueDataBase::getGreatestIndexNotNullIf(
    const IColumn & column, const UInt8 * __restrict null_map, const UInt8 * __restrict if_map, size_t row_begin, size_t row_end)
{
    size_t index = row_begin;
    while ((index < row_end) && ((if_map && if_map[index] == 0) || (null_map && null_map[index] != 0)))
        index++;
    if (index >= row_end)
        return std::nullopt;

    for (size_t i = index + 1; i < row_end; i++)
        if ((!if_map || if_map[i] != 0) && (!null_map || null_map[i] == 0) && (column.compareAt(i, index, column, nan_direction_hint) > 0))
            index = i;
    return {index};
}

void SingleValueDataBase::setSmallest(const IColumn & column, size_t row_begin, size_t row_end, Arena * arena)
{
    auto index = getSmallestIndex(column, row_begin, row_end);
    if (index)
        setIfSmaller(column, *index, arena);
}

void SingleValueDataBase::setGreatest(const IColumn & column, size_t row_begin, size_t row_end, Arena * arena)
{
    auto index = getGreatestIndex(column, row_begin, row_end);
    if (index)
        setIfGreater(column, *index, arena);
}

void SingleValueDataBase::setSmallestNotNullIf(
    const IColumn & column,
    const UInt8 * __restrict null_map,
    const UInt8 * __restrict if_map,
    size_t row_begin,
    size_t row_end,
    Arena * arena)
{
    auto index = getSmallestIndexNotNullIf(column, null_map, if_map, row_begin, row_end);
    if (index)
        setIfSmaller(column, *index, arena);
}

void SingleValueDataBase::setGreatestNotNullIf(
    const IColumn & column,
    const UInt8 * __restrict null_map,
    const UInt8 * __restrict if_map,
    size_t row_begin,
    size_t row_end,
    Arena * arena)
{
    auto index = getGreatestIndexNotNullIf(column, null_map, if_map, row_begin, row_end);
    if (index)
        setIfGreater(column, *index, arena);
}


template <typename T>
bool SingleValueDataFixed<T>::setIfSmaller(const IColumn & column, size_t row_num, Arena * arena)
{
    if (!has() || assert_cast<const ColVecType &>(column).getData()[row_num] < value)
    {
        set(column, row_num, arena);
        return true;
    }
    else
        return false;
}

template <typename T>
bool SingleValueDataFixed<T>::setIfGreater(const IColumn & column, size_t row_num, Arena * arena)
{
    if (!has() || assert_cast<const ColVecType &>(column).getData()[row_num] > value)
    {
        set(column, row_num, arena);
        return true;
    }
    else
        return false;
}

template <typename T>
void SingleValueDataFixed<T>::setSmallest(const IColumn & column, size_t row_begin, size_t row_end, Arena * arena)
{
    if (row_begin >= row_end)
        return;

    const auto & vec = assert_cast<const ColVecType &>(column);
    if constexpr (has_find_extreme_implementation<T>)
    {
        std::optional<T> opt = findExtremeMin(vec.getData().data(), row_begin, row_end);
        if (opt.has_value())
            setIfSmaller(*opt);
    }
    else
    {
        for (size_t i = row_begin; i < row_end; i++)
            setIfSmaller(column, i, arena);
    }
}

template <typename T>
void SingleValueDataFixed<T>::setGreatest(const IColumn & column, size_t row_begin, size_t row_end, Arena * arena)
{
    if (row_begin >= row_end)
        return;

    const auto & vec = assert_cast<const ColVecType &>(column);
    if constexpr (has_find_extreme_implementation<T>)
    {
        std::optional<T> opt = findExtremeMax(vec.getData().data(), row_begin, row_end);
        if (opt.has_value())
            setIfGreater(*opt);
    }
    else
    {
        for (size_t i = row_begin; i < row_end; i++)
            setIfGreater(column, i, arena);
    }
}

template <typename T>
void SingleValueDataFixed<T>::setSmallestNotNullIf(
    const IColumn & column,
    const UInt8 * __restrict null_map,
    const UInt8 * __restrict if_map,
    size_t row_begin,
    size_t row_end,
    Arena * arena)
{
    chassert(if_map || null_map);

    const auto & vec = assert_cast<const ColVecType &>(column);
    if constexpr (has_find_extreme_implementation<T>)
    {
        std::optional<T> opt;
        if (!if_map)
            opt = findExtremeMinNotNull(vec.getData().data(), null_map, row_begin, row_end);
        else if (!null_map)
            opt = findExtremeMinIf(vec.getData().data(), if_map, row_begin, row_end);
        else
        {
            auto final_flags = mergeIfAndNullFlags(null_map, if_map, row_begin, row_end);
            opt = findExtremeMinIf(vec.getData().data(), if_map, row_begin, row_end);
        }

        if (opt.has_value())
            setIfSmaller(*opt);
    }
    else
    {
        size_t index = row_begin;
        while ((index < row_end) && ((if_map && if_map[index] == 0) || (null_map && null_map[index] != 0)))
            index++;
        if (index >= row_end)
            return;

        setIfSmaller(column, index, arena);

        for (size_t i = index + 1; i < row_end; i++)
            if ((!if_map || if_map[i] != 0) && (!null_map || null_map[i] == 0))
                setIfSmaller(column, i, arena);
    }
}

template <typename T>
void SingleValueDataFixed<T>::setGreatestNotNullIf(
    const IColumn & column,
    const UInt8 * __restrict null_map,
    const UInt8 * __restrict if_map,
    size_t row_begin,
    size_t row_end,
    Arena * arena)
{
    chassert(if_map || null_map);

    const auto & vec = assert_cast<const ColVecType &>(column);
    if constexpr (has_find_extreme_implementation<T>)
    {
        std::optional<T> opt;
        if (!if_map)
            opt = findExtremeMaxNotNull(vec.getData().data(), null_map, row_begin, row_end);
        else if (!null_map)
            opt = findExtremeMaxIf(vec.getData().data(), if_map, row_begin, row_end);
        else
        {
            auto final_flags = mergeIfAndNullFlags(null_map, if_map, row_begin, row_end);
            opt = findExtremeMaxIf(vec.getData().data(), if_map, row_begin, row_end);
        }

        if (opt.has_value())
            setIfGreater(*opt);
    }
    else
    {
        size_t index = row_begin;
        while ((index < row_end) && ((if_map && if_map[index] == 0) || (null_map && null_map[index] != 0)))
            index++;
        if (index >= row_end)
            return;

        setIfSmaller(column, index, arena);

        for (size_t i = index + 1; i < row_end; i++)
            if ((!if_map || if_map[i] != 0) && (!null_map || null_map[i] == 0))
                setIfGreater(column, i, arena);
    }
}

template <typename T>
std::optional<size_t> SingleValueDataFixed<T>::getSmallestIndex(const IColumn & column, size_t row_begin, size_t row_end)
{
    if (row_begin >= row_end)
        return std::nullopt;

    const auto & vec = assert_cast<const ColVecType &>(column);
    if constexpr (has_find_extreme_implementation<T>)
    {
        std::optional<T> opt = findExtremeMin(vec.getData().data(), row_begin, row_end);
        if (!opt || (has() && value <= opt))
            return std::nullopt;

        /// TODO: Implement findExtremeMinIndex to do the lookup properly (with SIMD and batching)
        for (size_t i = row_begin; i < row_end; i++)
            if (vec.getData()[i] == *opt)
                return i;
        return row_end;
    }
    else
    {
        size_t index = row_begin;
        for (size_t i = index + 1; i < row_end; i++)
            if (vec.getData()[i] < vec.getData()[index])
                index = i;
        return index;
    }
}

template <typename T>
std::optional<size_t> SingleValueDataFixed<T>::getGreatestIndex(const IColumn & column, size_t row_begin, size_t row_end)
{
    if (row_begin >= row_end)
        return std::nullopt;

    const auto & vec = assert_cast<const ColVecType &>(column);
    if constexpr (has_find_extreme_implementation<T>)
    {
        std::optional<T> opt = findExtremeMax(vec.getData().data(), row_begin, row_end);
        if (!opt || (has() && value >= opt))
            return std::nullopt;

        /// TODO: Implement findExtremeMaxIndex to do the lookup properly (with SIMD and batching)
        for (size_t i = row_begin; i < row_end; i++)
            if (vec.getData()[i] == *opt)
                return i;
        return row_end;
    }
    else
    {
        size_t index = row_begin;
        for (size_t i = index + 1; i < row_end; i++)
            if (vec.getData()[i] > vec.getData()[index])
                index = i;
        return index;
    }
}


#define DISPATCH(TYPE) template struct SingleValueDataFixed<TYPE>;

FOR_SINGLE_VALUE_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH


namespace
{

struct StringValueCompatibility
{
    /// Old versions used to store terminating null-character in SingleValueDataString.
    /// Then -WithTerminatingZero methods were removed from IColumn interface,
    /// because these methods are quite dangerous and easy to misuse. It introduced incompatibility.
    /// See https://github.com/ClickHouse/ClickHouse/pull/41431 and https://github.com/ClickHouse/ClickHouse/issues/42916
    /// Here we keep these functions for compatibility.
    /// It's safe because there's no way unsanitized user input (without \0 at the end) can reach these functions.

    static StringRef getDataAtWithTerminatingZero(const ColumnString & column, size_t n)
    {
        auto res = column.getDataAt(n);
        /// ColumnString always reserves extra byte for null-character after string.
        /// But getDataAt returns StringRef without the null-character. Let's add it.
        chassert(res.data[res.size] == '\0');
        ++res.size;
        return res;
    }

    static void insertDataWithTerminatingZero(ColumnString & column, const char * pos, size_t length)
    {
        /// String already has terminating null-character.
        /// But insertData will add another one unconditionally. Trim existing null-character to avoid duplication.
        chassert(0 < length);
        chassert(pos[length - 1] == '\0');
        column.insertData(pos, length - 1);
    }
};

}


void SingleValueDataString::insertResultInto(DB::IColumn & to) const
{
    if (has())
        StringValueCompatibility::insertDataWithTerminatingZero(assert_cast<ColumnString &>(to), getData(), size);
    else
        assert_cast<ColumnString &>(to).insertDefault();
}

void SingleValueDataString::write(WriteBuffer & buf, const ISerialization & /*serialization*/) const
{
    if (unlikely(MAX_STRING_SIZE < size))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "String size is too big ({}), it's a bug", size);

    /// For serialization we use signed Int32 (for historical reasons), -1 means "no value"
    Int32 size_to_write = size ? size : -1;
    writeBinaryLittleEndian(size_to_write, buf);
    if (has())
        buf.write(getData(), size);
}

void SingleValueDataString::read(ReadBuffer & buf, const ISerialization & /*serialization*/, Arena * arena)
{
    /// For serialization we use signed Int32 (for historical reasons), -1 means "no value"
    Int32 rhs_size_signed;
    readBinaryLittleEndian(rhs_size_signed, buf);

    if (rhs_size_signed < 0)
    {
        /// Don't free large_data here.
        size = 0;
        return;
    }

    UInt32 rhs_size = rhs_size_signed;
    if (rhs_size <= MAX_SMALL_STRING_SIZE)
    {
        /// Don't free large_data here.
        size = rhs_size;
        buf.readStrict(small_data, size);
    }
    else
    {
        /// Reserve one byte more for null-character
        allocateLargeDataIfNeeded(rhs_size + 1, arena);
        size = rhs_size;
        buf.readStrict(large_data, size);
    }

    /// Check if the string we read is null-terminated (getDataMutable does not have the assertion)
    if (0 < size && getDataMutable()[size - 1] == '\0')
        return;

    /// It's not null-terminated, but it must be (for historical reasons). There are two variants:
    /// - The value was serialized by one of the incompatible versions of ClickHouse. We had some range of versions
    ///   that used to serialize SingleValueDataString without terminating '\0'. Let's just append it.
    /// - An attacker sent crafted data. Sanitize it and append '\0'.
    /// In all other cases the string must be already null-terminated.

    /// NOTE We cannot add '\0' unconditionally, because it will be duplicated.
    /// NOTE It's possible that a string that actually ends with '\0' was written by one of the incompatible versions.
    ///      Unfortunately, we cannot distinguish it from normal string written by normal version.
    ///      So such strings will be trimmed.

    if (size == MAX_SMALL_STRING_SIZE)
    {
        /// Special case: We have to move value to large_data
        allocateLargeDataIfNeeded(size + 1, arena);
        memcpy(large_data, small_data, size);
    }

    /// We have enough space to append
    ++size;
    getDataMutable()[size - 1] = '\0';
}

bool SingleValueDataString::isEqualTo(const DB::IColumn & column, size_t row_num) const
{
    return has()
        && StringValueCompatibility::getDataAtWithTerminatingZero(assert_cast<const ColumnString &>(column), row_num) == getStringRef();
}

bool SingleValueDataString::isEqualTo(const SingleValueDataBase & other) const
{
    auto const & to = assert_cast<const Self &>(other);
    return has() && to.getStringRef() == getStringRef();
}

void SingleValueDataString::set(const IColumn & column, size_t row_num, Arena * arena)
{
    changeImpl(StringValueCompatibility::getDataAtWithTerminatingZero(assert_cast<const ColumnString &>(column), row_num), arena);
}

void SingleValueDataString::set(const SingleValueDataBase & other, Arena * arena)
{
    auto const & to = assert_cast<const Self &>(other);
    if (to.has())
        changeImpl(to.getStringRef(), arena);
}

bool SingleValueDataString::setIfSmaller(const IColumn & column, size_t row_num, Arena * arena)
{
    if (!has()
        || StringValueCompatibility::getDataAtWithTerminatingZero(assert_cast<const ColumnString &>(column), row_num) < getStringRef())
    {
        set(column, row_num, arena);
        return true;
    }
    else
        return false;
}

bool SingleValueDataString::setIfSmaller(const SingleValueDataBase & other, Arena * arena)
{
    auto const & to = assert_cast<const Self &>(other);
    if (to.has() && (!has() || to.getStringRef() < getStringRef()))
    {
        changeImpl(to.getStringRef(), arena);
        return true;
    }
    else
        return false;
}


bool SingleValueDataString::setIfGreater(const IColumn & column, size_t row_num, Arena * arena)
{
    if (!has()
        || StringValueCompatibility::getDataAtWithTerminatingZero(assert_cast<const ColumnString &>(column), row_num) > getStringRef())
    {
        set(column, row_num, arena);
        return true;
    }
    else
        return false;
}

bool SingleValueDataString::setIfGreater(const SingleValueDataBase & other, Arena * arena)
{
    auto const & to = assert_cast<const Self &>(other);
    if (to.has() && (!has() || to.getStringRef() > getStringRef()))
    {
        changeImpl(to.getStringRef(), arena);
        return true;
    }
    else
        return false;
}


void generateSingleValueFromTypeIndex(TypeIndex idx, SingleValueDataBase::memory_block & data)
{
#define DISPATCH(TYPE) \
    if (idx == TypeIndex::TYPE) \
    { \
        static_assert(sizeof(SingleValueDataFixed<TYPE>) <= SingleValueDataBase::MAX_STORAGE_SIZE); \
        new (data.memory) SingleValueDataFixed<TYPE>(); \
        return; \
    }

    FOR_SINGLE_VALUE_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH

    if (idx == TypeIndex::Date)
    {
        static_assert(sizeof(SingleValueDataFixed<DataTypeDate::FieldType>) <= SingleValueDataBase::MAX_STORAGE_SIZE);
        new (data.memory) SingleValueDataFixed<DataTypeDate::FieldType>;
        return;
    }
    if (idx == TypeIndex::DateTime)
    {
        static_assert(sizeof(SingleValueDataFixed<DataTypeDateTime::FieldType>) <= SingleValueDataBase::MAX_STORAGE_SIZE);
        new (data.memory) SingleValueDataFixed<DataTypeDateTime::FieldType>;
        return;
    }
    if (idx == TypeIndex::String)
    {
        static_assert(sizeof(SingleValueDataString) <= SingleValueDataBase::MAX_STORAGE_SIZE);
        new (data.memory) SingleValueDataString;
        return;
    }
    static_assert(sizeof(SingleValueDataGeneric) <= SingleValueDataBase::MAX_STORAGE_SIZE);
    new (data.memory) SingleValueDataGeneric;
}
}
