#include <AggregateFunctions/SingleValueData.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/Arena.h>
#include <Common/assert_cast.h>
#include <Common/findExtreme.h>

#if USE_EMBEDDED_COMPILER
#    include <DataTypes/Native.h>
#    include <llvm/IR/IRBuilder.h>
#endif

#include <cstring>
#include <type_traits>


namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int TOO_LARGE_STRING_SIZE;
extern const int NOT_IMPLEMENTED;
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

std::optional<size_t> SingleValueDataBase::getSmallestIndex(const IColumn & column, size_t row_begin, size_t row_end) const
{
    static constexpr int nan_null_direction_hint = 1;

    if (row_begin >= row_end)
        return std::nullopt;

    /// TODO: Introduce row_begin and row_end to getPermutation
    if (row_begin != 0 || row_end != column.size())
    {
        size_t index = row_begin;
        for (size_t i = index + 1; i < row_end; i++)
            if ((column.compareAt(i, index, column, nan_null_direction_hint) < 0))
                index = i;
        return {index};
    }

    constexpr IColumn::PermutationSortDirection direction = IColumn::PermutationSortDirection::Ascending;
    constexpr IColumn::PermutationSortStability stability = IColumn::PermutationSortStability::Unstable;
    IColumn::Permutation permutation;
    constexpr UInt64 limit = 1;
    column.getPermutation(direction, stability, limit, nan_null_direction_hint, permutation);
    return {permutation[0]};
}

std::optional<size_t> SingleValueDataBase::getGreatestIndex(const IColumn & column, size_t row_begin, size_t row_end) const
{
    static constexpr int nan_null_direction_hint = -1;

    if (row_begin >= row_end)
        return std::nullopt;

    /// TODO: Introduce row_begin and row_end to getPermutation
    if (row_begin != 0 || row_end != column.size())
    {
        size_t index = row_begin;
        for (size_t i = index + 1; i < row_end; i++)
            if ((column.compareAt(i, index, column, nan_null_direction_hint) > 0))
                index = i;
        return {index};
    }

    constexpr IColumn::PermutationSortDirection direction = IColumn::PermutationSortDirection::Descending;
    constexpr IColumn::PermutationSortStability stability = IColumn::PermutationSortStability::Unstable;
    IColumn::Permutation permutation;
    constexpr UInt64 limit = 1;
    column.getPermutation(direction, stability, limit, nan_null_direction_hint, permutation);
    return {permutation[0]};
}

std::optional<size_t> SingleValueDataBase::getSmallestIndexNotNullIf(
    const IColumn & column, const UInt8 * __restrict null_map, const UInt8 * __restrict if_map, size_t row_begin, size_t row_end) const
{
    static constexpr int nan_null_direction_hint = 1;

    size_t index = row_begin;
    while ((index < row_end) && ((if_map && if_map[index] == 0) || (null_map && null_map[index] != 0)))
        index++;
    if (index >= row_end)
        return std::nullopt;

    for (size_t i = index + 1; i < row_end; i++)
        if ((!if_map || if_map[i] != 0) && (!null_map || null_map[i] == 0) && (column.compareAt(i, index, column, nan_null_direction_hint) < 0))
            index = i;
    return {index};
}

std::optional<size_t> SingleValueDataBase::getGreatestIndexNotNullIf(
    const IColumn & column, const UInt8 * __restrict null_map, const UInt8 * __restrict if_map, size_t row_begin, size_t row_end) const
{
    static constexpr int nan_null_direction_hint = -1;

    size_t index = row_begin;
    while ((index < row_end) && ((if_map && if_map[index] == 0) || (null_map && null_map[index] != 0)))
        index++;
    if (index >= row_end)
        return std::nullopt;

    for (size_t i = index + 1; i < row_end; i++)
        if ((!if_map || if_map[i] != 0) && (!null_map || null_map[i] == 0) && (column.compareAt(i, index, column, nan_null_direction_hint) > 0))
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
void SingleValueDataFixed<T>::insertResultInto(IColumn & to, const DataTypePtr &) const
{
    /// value is set to 0 in the constructor (also with JIT), so no need to check has_data()
    chassert(has() || value == T{});
    assert_cast<ColVecType &>(to).getData().push_back(value);
}

template <typename T>
void SingleValueDataFixed<T>::write(WriteBuffer & buf, const ISerialization &) const
{
    writeBinary(has(), buf);
    if (has())
        writeBinaryLittleEndian(value, buf);
}

template <typename T>
void SingleValueDataFixed<T>::read(ReadBuffer & buf, const ISerialization &, const DataTypePtr &, Arena *)
{
    readBinary(has_value, buf);
    if (has())
        readBinaryLittleEndian(value, buf);
}

template <typename T>
bool SingleValueDataFixed<T>::isEqualTo(const IColumn & column, size_t index) const
{
    return has() && assert_cast<const ColVecType &>(column).getData()[index] == value;
}

template <typename T>
bool SingleValueDataFixed<T>::isEqualTo(const SingleValueDataFixed<T> & to) const
{
    return has() && to.has() && to.value == value;
}

template <typename T>
void SingleValueDataFixed<T>::set(const IColumn & column, size_t row_num, Arena *)
{
    has_value = true;
    value = assert_cast<const ColVecType &>(column).getData()[row_num];
}

template <typename T>
void SingleValueDataFixed<T>::set(const SingleValueDataFixed<T> & to, Arena *)
{
    if (to.has())
    {
        has_value = true;
        value = to.value;
    }
}

template <typename T>
bool SingleValueDataFixed<T>::setIfSmaller(const T & to)
{
    if (!has_value || to < value)
    {
        has_value = true;
        value = to;
        return true;
    }
    return false;
}

template <typename T>
bool SingleValueDataFixed<T>::setIfGreater(const T & to)
{
    if (!has_value || to > value)
    {
        has_value = true;
        value = to;
        return true;
    }
    return false;
}

template <typename T>
bool SingleValueDataFixed<T>::setIfSmaller(const SingleValueDataFixed<T> & to, Arena * arena)
{
    if (to.has() && (!has() || to.value < value))
    {
        set(to, arena);
        return true;
    }
    return false;
}

template <typename T>
bool SingleValueDataFixed<T>::setIfGreater(const SingleValueDataFixed<T> & to, Arena * arena)
{
    if (to.has() && (!has() || to.value > value))
    {
        set(to, arena);
        return true;
    }
    return false;
}

template <typename T>
bool SingleValueDataFixed<T>::setIfSmaller(const IColumn & column, size_t row_num, Arena * arena)
{
    if (!has() || assert_cast<const ColVecType &>(column).getData()[row_num] < value)
    {
        set(column, row_num, arena);
        return true;
    }
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
    return false;
}

template <typename T>
void SingleValueDataFixed<T>::setSmallest(const IColumn & column, size_t row_begin, size_t row_end, Arena * arena)
{
    if (row_begin >= row_end)
        return;

    const auto & vec = assert_cast<const ColVecType &>(column);
    if constexpr (has_find_extreme_implementation<T> || underlying_has_find_extreme_implementation<T>)
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
    if constexpr (has_find_extreme_implementation<T> || underlying_has_find_extreme_implementation<T>)
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
    if constexpr (has_find_extreme_implementation<T> || underlying_has_find_extreme_implementation<T>)
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
    if constexpr (has_find_extreme_implementation<T> || underlying_has_find_extreme_implementation<T>)
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

        setIfGreater(column, index, arena);

        for (size_t i = index + 1; i < row_end; i++)
            if ((!if_map || if_map[i] != 0) && (!null_map || null_map[i] == 0))
                setIfGreater(column, i, arena);
    }
}

template <typename T>
std::optional<size_t> SingleValueDataFixed<T>::getSmallestIndex(const IColumn & column, size_t row_begin, size_t row_end) const
{
    if (row_begin >= row_end)
        return std::nullopt;

    const auto & vec = assert_cast<const ColVecType &>(column);
    if constexpr (has_find_extreme_implementation<T> || underlying_has_find_extreme_implementation<T>)
    {
        return findExtremeMinIndex(vec.getData().data(), row_begin, row_end);
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
std::optional<size_t> SingleValueDataFixed<T>::getGreatestIndex(const IColumn & column, size_t row_begin, size_t row_end) const
{
    if (row_begin >= row_end)
        return std::nullopt;

    const auto & vec = assert_cast<const ColVecType &>(column);
    if constexpr (has_find_extreme_implementation<T> || underlying_has_find_extreme_implementation<T>)
    {
        return findExtremeMaxIndex(vec.getData().data(), row_begin, row_end);
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

template <typename T>
std::optional<size_t> SingleValueDataFixed<T>::getSmallestIndexNotNullIf(
    const IColumn & column, const UInt8 * __restrict null_map, const UInt8 * __restrict if_map, size_t row_begin, size_t row_end) const
{
    if (row_begin >= row_end)
        return std::nullopt;

    const auto & vec = assert_cast<const ColVecType &>(column);
    const auto & vec_data = vec.getData();

    if constexpr (has_find_extreme_implementation<T> || underlying_has_find_extreme_implementation<T>)
    {
        std::optional<T> opt;
        if (!if_map)
        {
            opt = findExtremeMinNotNull(vec.getData().data(), null_map, row_begin, row_end);
            if (!opt.has_value())
                return std::nullopt;
            T smallest = *opt;
            for (size_t i = row_begin; i < row_end; i++)
            {
                if constexpr (is_floating_point<T>)
                {
                    /// We search for the exact byte representation, not the default floating point equal, otherwise we might not find the value (NaN)
                    static_assert(std::is_trivial_v<T> && std::is_standard_layout_v<T>);
                    if (!null_map[i] && std::memcmp(&vec_data[i], &smallest, sizeof(T)) == 0) // NOLINT (we are comparing FP with memcmp on purpose)
                        return {i};
                }
                else
                {
                    if (!null_map[i] && vec_data[i] == smallest)
                        return {i};
                }
            }
        }
        else if (!null_map)
        {
            opt = findExtremeMinIf(vec.getData().data(), if_map, row_begin, row_end);
            if (!opt.has_value())
                return std::nullopt;
            T smallest = *opt;
            for (size_t i = row_begin; i < row_end; i++)
            {
                if constexpr (is_floating_point<T>)
                {
                    static_assert(std::is_trivial_v<T> && std::is_standard_layout_v<T>);
                    if (if_map[i] && std::memcmp(&vec_data[i], &smallest, sizeof(T)) == 0) // NOLINT (we are comparing FP with memcmp on purpose)
                        return {i};
                }
                else
                {
                    if (if_map[i] && vec_data[i] == smallest)
                        return {i};
                }
            }
        }
        else
        {
            auto final_flags = mergeIfAndNullFlags(null_map, if_map, row_begin, row_end);
            opt = findExtremeMinIf(vec.getData().data(), final_flags.get(), row_begin, row_end);
            if (!opt.has_value())
                return std::nullopt;
            T smallest = *opt;
            for (size_t i = row_begin; i < row_end; i++)
            {
                if constexpr (is_floating_point<T>)
                {
                    static_assert(std::is_trivial_v<T> && std::is_standard_layout_v<T>);
                    if (final_flags[i] && std::memcmp(&vec_data[i], &smallest, sizeof(T)) == 0) // NOLINT (we are comparing FP with memcmp on purpose)
                        return {i};
                }
                else
                {
                    if (final_flags[i] && vec_data[i] == smallest)
                        return {i};
                }
            }
        }
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to find index");
    }
    else
    {
        size_t index = row_begin;
        while ((index < row_end) && ((if_map && if_map[index] == 0) || (null_map && null_map[index] != 0)))
            index++;
        if (index >= row_end)
            return std::nullopt;

        for (size_t i = index + 1; i < row_end; i++)
            if ((!if_map || if_map[i] != 0) && (!null_map || null_map[i] == 0) && (vec_data[i] < vec_data[index]))
                index = i;
        return {index};
    }
}

template <typename T>
std::optional<size_t> SingleValueDataFixed<T>::getGreatestIndexNotNullIf(
    const IColumn & column, const UInt8 * __restrict null_map, const UInt8 * __restrict if_map, size_t row_begin, size_t row_end) const
{
    if (row_begin >= row_end)
        return std::nullopt;

    const auto & vec = assert_cast<const ColVecType &>(column);
    const auto & vec_data = vec.getData();

    if constexpr (has_find_extreme_implementation<T> || underlying_has_find_extreme_implementation<T>)
    {
        std::optional<T> opt;
        if (!if_map)
        {
            opt = findExtremeMaxNotNull(vec.getData().data(), null_map, row_begin, row_end);
            if (!opt.has_value())
                return std::nullopt;
            T greatest = *opt;
            for (size_t i = row_begin; i < row_end; i++)
            {
                if constexpr (is_floating_point<T>)
                {
                    static_assert(std::is_trivial_v<T> && std::is_standard_layout_v<T>);
                    if (!null_map[i] && std::memcmp(&vec_data[i], &greatest, sizeof(T)) == 0) // NOLINT (we are comparing FP with memcmp on purpose)
                        return {i};
                }
                else
                {
                    if (!null_map[i] && vec_data[i] == greatest)
                        return {i};
                }
            }
        }
        else if (!null_map)
        {
            opt = findExtremeMaxIf(vec.getData().data(), if_map, row_begin, row_end);
            if (!opt.has_value())
                return std::nullopt;
            T greatest = *opt;
            for (size_t i = row_begin; i < row_end; i++)
            {
                if constexpr (is_floating_point<T>)
                {
                    static_assert(std::is_trivial_v<T> && std::is_standard_layout_v<T>);
                    if (if_map[i] && std::memcmp(&vec_data[i], &greatest, sizeof(T)) == 0) // NOLINT (we are comparing FP with memcmp on purpose)
                        return {i};
                }
                else
                {
                    if (if_map[i] && vec_data[i] == greatest)
                        return {i};
                }
            }
        }
        else
        {
            auto final_flags = mergeIfAndNullFlags(null_map, if_map, row_begin, row_end);
            opt = findExtremeMaxIf(vec.getData().data(), final_flags.get(), row_begin, row_end);
            if (!opt.has_value())
                return std::nullopt;
            T greatest = *opt;
            for (size_t i = row_begin; i < row_end; i++)
            {
                if constexpr (is_floating_point<T>)
                {
                    static_assert(std::is_trivial_v<T> && std::is_standard_layout_v<T>);
                    if (final_flags[i] && std::memcmp(&vec_data[i], &greatest, sizeof(T)) == 0) // NOLINT (we are comparing FP with memcmp on purpose)
                        return {i};
                }
                else
                {
                    if (final_flags[i] && vec_data[i] == greatest)
                        return {i};
                }
            }
        }
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to find index");
    }
    else
    {
        size_t index = row_begin;
        while ((index < row_end) && ((if_map && if_map[index] == 0) || (null_map && null_map[index] != 0)))
            index++;
        if (index >= row_end)
            return std::nullopt;

        for (size_t i = index + 1; i < row_end; i++)
            if ((!if_map || if_map[i] != 0) && (!null_map || null_map[i] == 0) && (vec_data[i] > vec_data[index]))
                index = i;
        return {index};
    }
}


#if USE_EMBEDDED_COMPILER

template <typename T>
bool SingleValueDataFixed<T>::isCompilable(const IDataType & type)
{
    return canBeNativeType(type);
}

template <typename T>
llvm::Value * SingleValueDataFixed<T>::getValuePtrFromAggregateDataPtr(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr)
{
    llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);
    auto * value_ptr = b.CreateConstInBoundsGEP1_64(b.getInt8Ty(), aggregate_data_ptr, value_offset);
    return value_ptr;
}

template <typename T>
llvm::Value * SingleValueDataFixed<T>::getValueFromAggregateDataPtr(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr)
{
    llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);
    auto * type = toNativeType<T>(builder);
    auto * value_ptr = getValuePtrFromAggregateDataPtr(builder, aggregate_data_ptr);
    return b.CreateLoad(type, value_ptr);
}

template <typename T>
llvm::Value * SingleValueDataFixed<T>::getHasValuePtrFromAggregateDataPtr(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr)
{
    llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);
    auto * has_value_ptr = b.CreateConstInBoundsGEP1_64(b.getInt8Ty(), aggregate_data_ptr, has_value_offset);
    return has_value_ptr;
}

template <typename T>
llvm::Value * SingleValueDataFixed<T>::getHasValueFromAggregateDataPtr(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr)
{
    llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);
    auto * has_value_ptr = getHasValuePtrFromAggregateDataPtr(builder, aggregate_data_ptr);
    return b.CreateLoad(b.getInt1Ty(), has_value_ptr);
}

template <typename T>
void SingleValueDataFixed<T>::compileCreate(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr)
{
    llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);
    /// We initialize everything to 0 so both has_data and value are initialized
    /// Note that getResult uses the knowledge that value is 0 with no data, both with and without JIT,
    /// to skip a check
    b.CreateMemSet(
        b.CreateConstInBoundsGEP1_64(b.getInt8Ty(), aggregate_data_ptr, 0),
        llvm::ConstantInt::get(b.getInt8Ty(), 0),
        sizeof(SingleValueDataFixed<T>),
        llvm::assumeAligned(alignof(SingleValueDataFixed<T>)));
}

template <typename T>
llvm::Value * SingleValueDataFixed<T>::compileGetResult(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr)
{
    /// value is set to 0 in the constructor, so no need to check has_data()
    return getValueFromAggregateDataPtr(builder, aggregate_data_ptr);
}

template <typename T>
void SingleValueDataFixed<T>::compileSetValueFromNumber(
    llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr, llvm::Value * value_to_check)
{
    llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

    auto * has_value_ptr = getHasValuePtrFromAggregateDataPtr(builder, aggregate_data_ptr);
    b.CreateStore(b.getTrue(), has_value_ptr);

    auto * value_ptr = getValuePtrFromAggregateDataPtr(b, aggregate_data_ptr);
    b.CreateStore(value_to_check, value_ptr);
}

template <typename T>
void SingleValueDataFixed<T>::compileSetValueFromAggregation(
    llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr, llvm::Value * aggregate_data_src_ptr)
{
    auto * value_src = getValueFromAggregateDataPtr(builder, aggregate_data_src_ptr);
    compileSetValueFromNumber(builder, aggregate_data_ptr, value_src);
}

template <typename T>
void SingleValueDataFixed<T>::compileAny(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr, llvm::Value * value_to_check)
{
    llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

    auto * head = b.GetInsertBlock();
    auto * join_block = llvm::BasicBlock::Create(head->getContext(), "join_block", head->getParent());
    auto * if_should_change = llvm::BasicBlock::Create(head->getContext(), "if_should_change", head->getParent());

    auto * has_value_value = getHasValueFromAggregateDataPtr(b, aggregate_data_ptr);
    b.CreateCondBr(has_value_value, join_block, if_should_change);

    b.SetInsertPoint(if_should_change);
    compileSetValueFromNumber(builder, aggregate_data_ptr, value_to_check);
    b.CreateBr(join_block);

    b.SetInsertPoint(join_block);
}

template <typename T>
void SingleValueDataFixed<T>::compileAnyMerge(
    llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_dst_ptr, llvm::Value * aggregate_data_src_ptr)
{
    llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

    auto * head = b.GetInsertBlock();
    auto * join_block = llvm::BasicBlock::Create(head->getContext(), "join_block", head->getParent());
    auto * if_should_change = llvm::BasicBlock::Create(head->getContext(), "if_should_change", head->getParent());

    auto * has_value_dst = getHasValueFromAggregateDataPtr(b, aggregate_data_dst_ptr);
    auto * has_value_src = getHasValueFromAggregateDataPtr(b, aggregate_data_src_ptr);
    b.CreateCondBr(b.CreateAnd(b.CreateNot(has_value_dst), has_value_src), if_should_change, join_block);

    b.SetInsertPoint(if_should_change);
    compileSetValueFromAggregation(builder, aggregate_data_dst_ptr, aggregate_data_src_ptr);
    b.CreateBr(join_block);

    b.SetInsertPoint(join_block);
}

template <typename T>
void SingleValueDataFixed<T>::compileAnyLast(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr, llvm::Value * value_to_check)
{
    compileSetValueFromNumber(builder, aggregate_data_ptr, value_to_check);
}

template <typename T>
void SingleValueDataFixed<T>::compileAnyLastMerge(
    llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_dst_ptr, llvm::Value * aggregate_data_src_ptr)
{
    llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

    auto * head = b.GetInsertBlock();
    auto * join_block = llvm::BasicBlock::Create(head->getContext(), "join_block", head->getParent());
    auto * if_should_change = llvm::BasicBlock::Create(head->getContext(), "if_should_change", head->getParent());

    auto * has_value_src = getHasValueFromAggregateDataPtr(b, aggregate_data_src_ptr);
    b.CreateCondBr(has_value_src, if_should_change, join_block);

    b.SetInsertPoint(if_should_change);
    compileSetValueFromAggregation(builder, aggregate_data_dst_ptr, aggregate_data_src_ptr);
    b.CreateBr(join_block);

    b.SetInsertPoint(join_block);
}

template <typename T>
template <bool isMin>
void SingleValueDataFixed<T>::compileMinMax(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr, llvm::Value * value_to_check)
{
    llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

    auto * has_value_value = getHasValueFromAggregateDataPtr(b, aggregate_data_ptr);
    auto * value = getValueFromAggregateDataPtr(b, aggregate_data_ptr);

    auto * head = b.GetInsertBlock();

    auto * join_block = llvm::BasicBlock::Create(head->getContext(), "join_block", head->getParent());
    auto * if_should_change = llvm::BasicBlock::Create(head->getContext(), "if_should_change", head->getParent());

    constexpr auto is_signed = std::numeric_limits<T>::is_signed;

    llvm::Value * should_change_after_comparison = nullptr;

    if constexpr (isMin)
    {
        if (value_to_check->getType()->isIntegerTy())
            should_change_after_comparison = is_signed ? b.CreateICmpSLT(value_to_check, value) : b.CreateICmpULT(value_to_check, value);
        else
            should_change_after_comparison = b.CreateFCmpOLT(value_to_check, value);
    }
    else
    {
        if (value_to_check->getType()->isIntegerTy())
            should_change_after_comparison = is_signed ? b.CreateICmpSGT(value_to_check, value) : b.CreateICmpUGT(value_to_check, value);
        else
            should_change_after_comparison = b.CreateFCmpOGT(value_to_check, value);
    }

    b.CreateCondBr(b.CreateOr(b.CreateNot(has_value_value), should_change_after_comparison), if_should_change, join_block);

    b.SetInsertPoint(if_should_change);
    compileSetValueFromNumber(builder, aggregate_data_ptr, value_to_check);
    b.CreateBr(join_block);

    b.SetInsertPoint(join_block);
}


template <typename T>
template <bool isMin>
void SingleValueDataFixed<T>::compileMinMaxMerge(
    llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_dst_ptr, llvm::Value * aggregate_data_src_ptr)
{
    llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

    auto * has_value_dst = getHasValueFromAggregateDataPtr(b, aggregate_data_dst_ptr);
    auto * value_dst = getValueFromAggregateDataPtr(b, aggregate_data_dst_ptr);

    auto * has_value_src = getHasValueFromAggregateDataPtr(b, aggregate_data_src_ptr);
    auto * value_src = getValueFromAggregateDataPtr(b, aggregate_data_src_ptr);

    auto * head = b.GetInsertBlock();

    auto * join_block = llvm::BasicBlock::Create(head->getContext(), "join_block", head->getParent());
    auto * if_should_change = llvm::BasicBlock::Create(head->getContext(), "if_should_change", head->getParent());

    constexpr auto is_signed = std::numeric_limits<T>::is_signed;

    llvm::Value * should_change_after_comparison = nullptr;

    if constexpr (isMin)
    {
        if (value_src->getType()->isIntegerTy())
            should_change_after_comparison = is_signed ? b.CreateICmpSLT(value_src, value_dst) : b.CreateICmpULT(value_src, value_dst);
        else
            should_change_after_comparison = b.CreateFCmpOLT(value_src, value_dst);
    }
    else
    {
        if (value_src->getType()->isIntegerTy())
            should_change_after_comparison = is_signed ? b.CreateICmpSGT(value_src, value_dst) : b.CreateICmpUGT(
                    value_src, value_dst);
        else
            should_change_after_comparison = b.CreateFCmpOGT(value_src, value_dst);
    }

    b.CreateCondBr(
        b.CreateAnd(has_value_src, b.CreateOr(b.CreateNot(has_value_dst), should_change_after_comparison)), if_should_change, join_block);

    b.SetInsertPoint(if_should_change);
    compileSetValueFromAggregation(builder, aggregate_data_dst_ptr, aggregate_data_src_ptr);
    b.CreateBr(join_block);

    b.SetInsertPoint(join_block);
}

template <typename T>
void SingleValueDataFixed<T>::compileMin(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr, llvm::Value * value_to_check)
{
    return compileMinMax<true>(builder, aggregate_data_ptr, value_to_check);
}

template <typename T>
void SingleValueDataFixed<T>::compileMinMerge(
    llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_dst_ptr, llvm::Value * aggregate_data_src_ptr)
{
    return compileMinMaxMerge<true>(builder, aggregate_data_dst_ptr, aggregate_data_src_ptr);
}

template <typename T>
void SingleValueDataFixed<T>::compileMax(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr, llvm::Value * value_to_check)
{
    return compileMinMax<false>(builder, aggregate_data_ptr, value_to_check);
}

template <typename T>
void SingleValueDataFixed<T>::compileMaxMerge(
    llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_dst_ptr, llvm::Value * aggregate_data_src_ptr)
{
    return compileMinMaxMerge<false>(builder, aggregate_data_dst_ptr, aggregate_data_src_ptr);
}

#endif


#define DISPATCH(TYPE) template struct SingleValueDataFixed<TYPE>;

FOR_SINGLE_VALUE_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH


template <typename T>
SingleValueDataNumeric<T>::SingleValueDataNumeric()
{
    new (&memory) SingleValueDataFixed<T>;
}

/// No need to deallocate or do anything, since SingleValueDataFixed<T> uses only static storage
template <typename T>
SingleValueDataNumeric<T>::~SingleValueDataNumeric() = default;

template <typename T>
bool SingleValueDataNumeric<T>::has() const
{
    return memory.get().has();
}

template <typename T>
void SingleValueDataNumeric<T>::insertResultInto(IColumn & to, const DataTypePtr & type) const
{
    return memory.get().insertResultInto(to, type);
}

template <typename T>
void SingleValueDataNumeric<T>::write(WriteBuffer & buf, const ISerialization & serialization) const
{
    return memory.get().write(buf, serialization);
}

template <typename T>
void SingleValueDataNumeric<T>::read(ReadBuffer & buf, const ISerialization & serialization, const DataTypePtr & type, Arena * arena)
{
    return memory.get().read(buf, serialization, type, arena);
}

template <typename T>
bool SingleValueDataNumeric<T>::isEqualTo(const IColumn & column, size_t index) const
{
    return memory.get().isEqualTo(column, index);
}

template <typename T>
bool SingleValueDataNumeric<T>::isEqualTo(const SingleValueDataBase & to) const
{
    /// to.has() is checked in memory.get().isEqualTo
    auto const & other = assert_cast<const Self &>(to);
    return memory.get().isEqualTo(other.memory.get());
}

template <typename T>
void SingleValueDataNumeric<T>::set(const IColumn & column, size_t row_num, Arena * arena)
{
    return memory.get().set(column, row_num, arena);
}

template <typename T>
void SingleValueDataNumeric<T>::set(const SingleValueDataBase & to, Arena * arena)
{
    /// to.has() is checked in memory.get().set
    auto const & other = assert_cast<const Self &>(to);
    return memory.get().set(other.memory.get(), arena);
}

template <typename T>
bool SingleValueDataNumeric<T>::setIfSmaller(const SingleValueDataBase & to, Arena * arena)
{
    /// to.has() is checked in memory.get().setIfSmaller
    auto const & other = assert_cast<const Self &>(to);
    return memory.get().setIfSmaller(other.memory.get(), arena);
}

template <typename T>
bool SingleValueDataNumeric<T>::setIfGreater(const SingleValueDataBase & to, Arena * arena)
{
    /// to.has() is checked in memory.get().setIfGreater
    auto const & other = assert_cast<const Self &>(to);
    return memory.get().setIfGreater(other.memory.get(), arena);
}

template <typename T>
bool SingleValueDataNumeric<T>::setIfSmaller(const IColumn & column, size_t row_num, Arena * arena)
{
    return memory.get().setIfSmaller(column, row_num, arena);
}

template <typename T>
bool SingleValueDataNumeric<T>::setIfGreater(const IColumn & column, size_t row_num, Arena * arena)
{
    return memory.get().setIfGreater(column, row_num, arena);
}

template <typename T>
void SingleValueDataNumeric<T>::setSmallest(const IColumn & column, size_t row_begin, size_t row_end, Arena * arena)
{
    return memory.get().setSmallest(column, row_begin, row_end, arena);
}

template <typename T>
void SingleValueDataNumeric<T>::setGreatest(const IColumn & column, size_t row_begin, size_t row_end, Arena * arena)
{
    return memory.get().setGreatest(column, row_begin, row_end, arena);
}

template <typename T>
void SingleValueDataNumeric<T>::setSmallestNotNullIf(
    const IColumn & column,
    const UInt8 * __restrict null_map,
    const UInt8 * __restrict if_map,
    size_t row_begin,
    size_t row_end,
    Arena * arena)
{
    return memory.get().setSmallestNotNullIf(column, null_map, if_map, row_begin, row_end, arena);
}

template <typename T>
void SingleValueDataNumeric<T>::setGreatestNotNullIf(
    const IColumn & column,
    const UInt8 * __restrict null_map,
    const UInt8 * __restrict if_map,
    size_t row_begin,
    size_t row_end,
    Arena * arena)
{
    return memory.get().setGreatestNotNullIf(column, null_map, if_map, row_begin, row_end, arena);
}

template <typename T>
std::optional<size_t> SingleValueDataNumeric<T>::getSmallestIndex(const IColumn & column, size_t row_begin, size_t row_end) const
{
    return memory.get().getSmallestIndex(column, row_begin, row_end);
}

template <typename T>
std::optional<size_t> SingleValueDataNumeric<T>::getGreatestIndex(const IColumn & column, size_t row_begin, size_t row_end) const
{
    return memory.get().getGreatestIndex(column, row_begin, row_end);
}

template <typename T>
std::optional<size_t> SingleValueDataNumeric<T>::getSmallestIndexNotNullIf(
    const IColumn & column, const UInt8 * __restrict null_map, const UInt8 * __restrict if_map, size_t row_begin, size_t row_end) const
{
    return memory.get().getSmallestIndexNotNullIf(column, null_map, if_map, row_begin, row_end);
}

template <typename T>
std::optional<size_t> SingleValueDataNumeric<T>::getGreatestIndexNotNullIf(
    const IColumn & column, const UInt8 * __restrict null_map, const UInt8 * __restrict if_map, size_t row_begin, size_t row_end) const
{
    return memory.get().getGreatestIndexNotNullIf(column, null_map, if_map, row_begin, row_end);
}

#define DISPATCH(TYPE) template struct SingleValueDataNumeric<TYPE>;

FOR_SINGLE_VALUE_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH

/// String

char * SingleValueDataString::getDataMutable()
{
    return isSmall() ? small_data : large_data;
}

const char * SingleValueDataString::getData() const
{
    return isSmall() ? small_data : large_data;
}

StringRef SingleValueDataString::getStringRef() const
{
    return StringRef(getData(), size - 1);
}

void SingleValueDataString::allocateLargeDataIfNeeded(UInt32 size_to_reserve, Arena * arena)
{
    if (capacity < size_to_reserve)
    {
        if (unlikely(MAX_STRING_SIZE < size_to_reserve))
            throw Exception(
                ErrorCodes::TOO_LARGE_STRING_SIZE, "String size is too big ({}), maximum: {}", size_to_reserve, MAX_STRING_SIZE);

        size_t rounded_capacity = roundUpToPowerOfTwoOrZero(size_to_reserve);
        chassert(rounded_capacity <= MAX_STRING_SIZE + 1); /// rounded_capacity <= 2^31
        capacity = static_cast<UInt32>(rounded_capacity);

        /// Don't free large_data here.
        large_data = arena->alloc(capacity);
    }
}

void SingleValueDataString::changeImpl(StringRef value, Arena * arena)
{
    if (unlikely(MAX_STRING_SIZE < value.size))
        throw Exception(ErrorCodes::TOO_LARGE_STRING_SIZE, "String size is too big ({}), maximum: {}", value.size, MAX_STRING_SIZE);

    UInt32 value_size = static_cast<UInt32>(value.size);

    if (value_size <= MAX_SMALL_STRING_SIZE && isSmall())
    {
        /// Don't free large_data here.
        size = value_size + 1;

        if (value_size > 0)
            memcpy(small_data, value.data, value.size);
    }
    else
    {
        allocateLargeDataIfNeeded(value_size, arena);

        size = value_size + 1;
        memcpy(large_data, value.data, value.size);
    }
}

void SingleValueDataString::insertResultInto(IColumn & to, const DataTypePtr &) const
{
    if (has())
        assert_cast<ColumnString &>(to).insertData(getData(), size - 1);
    else
        assert_cast<ColumnString &>(to).insertDefault();
}

void SingleValueDataString::write(WriteBuffer & buf, const ISerialization & /*serialization*/) const
{
    if (unlikely(MAX_STRING_SIZE < size))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "String size is too big ({}), it's a bug", size);

    /// For serialization we use signed Int32 (for historical reasons), -1 means "no value"
    /// The strings are serialized as zero terminated.
    Int32 size_to_write = size ? size : -1;
    writeBinaryLittleEndian(size_to_write, buf);
    if (has())
    {
        buf.write(getData(), size - 1);
        buf.write('\0');
    }
}

void SingleValueDataString::read(ReadBuffer & buf, const ISerialization & /*serialization*/, const DataTypePtr & /*type*/, Arena * arena)
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

    /// Compatibility with an invalid format in certain old versions.
    if (rhs_size_signed == 0)
    {
        /// Don't free large_data here.
        size = 1;
        return;
    }

    /// The strings are serialized as zero terminated.
    char last_char;

    UInt32 rhs_size = rhs_size_signed;
    if (rhs_size <= MAX_SMALL_STRING_SIZE + 1 && isSmall())
    {
        /// Don't free large_data here.
        size = rhs_size;
        buf.readStrict(small_data, size - 1);
        readChar(last_char, buf);
    }
    else
    {
        size = rhs_size;
        allocateLargeDataIfNeeded(size - 1, arena);
        buf.readStrict(large_data, size - 1);
        readChar(last_char, buf);
    }

    /// Compatibility with an invalid format in certain old versions.
    if (last_char != 0)
    {
        if (size < MAX_SMALL_STRING_SIZE + 1 && isSmall())
        {
            small_data[size - 1] = last_char;
        }
        else if (size == MAX_SMALL_STRING_SIZE + 1 && isSmall())
        {
            String tmp;
            tmp.reserve(size);
            tmp.assign(small_data, size - 1);
            tmp += last_char;

            allocateLargeDataIfNeeded(size, arena);
            memcpy(large_data, tmp.data(), size);
        }
        else
        {
            large_data[size - 1] = last_char;
        }
        ++size;
    }
}

bool SingleValueDataString::isEqualTo(const IColumn & column, size_t row_num) const
{
    return has() && assert_cast<const ColumnString &>(column).getDataAt(row_num) == getStringRef();
}

bool SingleValueDataString::isEqualTo(const SingleValueDataBase & other) const
{
    auto const & to = assert_cast<const Self &>(other);
    return has() && to.has() && to.getStringRef() == getStringRef();
}

void SingleValueDataString::set(const IColumn & column, size_t row_num, Arena * arena)
{
    changeImpl(assert_cast<const ColumnString &>(column).getDataAt(row_num), arena);
}

void SingleValueDataString::set(const SingleValueDataBase & other, Arena * arena)
{
    auto const & to = assert_cast<const Self &>(other);
    if (to.has())
        changeImpl(to.getStringRef(), arena);
}

bool SingleValueDataString::setIfSmaller(const IColumn & column, size_t row_num, Arena * arena)
{
    if (!has() || assert_cast<const ColumnString &>(column).getDataAt(row_num) < getStringRef())
    {
        set(column, row_num, arena);
        return true;
    }
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
    return false;
}


bool SingleValueDataString::setIfGreater(const IColumn & column, size_t row_num, Arena * arena)
{
    if (!has() || assert_cast<const ColumnString &>(column).getDataAt(row_num) > getStringRef())
    {
        set(column, row_num, arena);
        return true;
    }
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
    return false;
}

/// Generic

void SingleValueDataGeneric::insertResultInto(IColumn & to, const DataTypePtr & type) const
{
    if (has())
        to.insert(value);
    else
        type->insertDefaultInto(to);
}

void SingleValueDataGeneric::write(WriteBuffer & buf, const ISerialization & serialization) const
{
    if (!value.isNull())
    {
        writeBinary(true, buf);
        serialization.serializeBinary(value, buf, {});
    }
    else
        writeBinary(false, buf);
}

void SingleValueDataGeneric::read(ReadBuffer & buf, const ISerialization & serialization, const DataTypePtr &, Arena *)
{
    bool is_not_null;
    readBinary(is_not_null, buf);

    if (is_not_null)
        serialization.deserializeBinary(value, buf, {});
}

bool SingleValueDataGeneric::isEqualTo(const IColumn & column, size_t row_num) const
{
    return has() && value == column[row_num];
}

bool SingleValueDataGeneric::isEqualTo(const SingleValueDataBase & other) const
{
    auto const & to = assert_cast<const Self &>(other);
    return has() && to.has() && to.value == value;
}

void SingleValueDataGeneric::set(const IColumn & column, size_t row_num, Arena *)
{
    column.get(row_num, value);
}

void SingleValueDataGeneric::set(const SingleValueDataBase & other, Arena *)
{
    auto const & to = assert_cast<const Self &>(other);
    if (other.has())
        value = to.value;
}

bool SingleValueDataGeneric::setIfSmaller(const IColumn & column, size_t row_num, Arena * arena)
{
    if (!has())
    {
        set(column, row_num, arena);
        return true;
    }

    Field new_value;
    column.get(row_num, new_value);
    if (new_value < value)
    {
        value = new_value;
        return true;
    }
    return false;
}

bool SingleValueDataGeneric::setIfSmaller(const SingleValueDataBase & other, Arena *)
{
    auto const & to = assert_cast<const Self &>(other);
    if (to.has() && (!has() || to.value < value))
    {
        value = to.value;
        return true;
    }
    return false;
}

bool SingleValueDataGeneric::setIfGreater(const IColumn & column, size_t row_num, Arena * arena)
{
    if (!has())
    {
        set(column, row_num, arena);
        return true;
    }

    Field new_value;
    column.get(row_num, new_value);
    if (new_value > value)
    {
        value = new_value;
        return true;
    }
    return false;
}

bool SingleValueDataGeneric::setIfGreater(const SingleValueDataBase & other, Arena *)
{
    auto const & to = assert_cast<const Self &>(other);
    if (to.has() && (!has() || to.value > value))
    {
        value = to.value;
        return true;
    }
    return false;
}

/// GenericWithColumn

void SingleValueDataGenericWithColumn::insertResultInto(IColumn & to, const DataTypePtr & type) const
{
    if (has())
        to.insertFrom(*value, 0);
    else
        type->insertDefaultInto(to);
}

void SingleValueDataGenericWithColumn::write(WriteBuffer & buf, const ISerialization & serialization) const
{
    if (value)
    {
        writeBinary(true, buf);
        serialization.serializeBinary(*value, 0, buf, {});
    }
    else
        writeBinary(false, buf);
}

void SingleValueDataGenericWithColumn::read(ReadBuffer & buf, const ISerialization & serialization, const DataTypePtr & type, Arena *)
{
    bool is_not_null;
    readBinary(is_not_null, buf);

    if (is_not_null)
    {
        auto new_value = type->createColumn();
        new_value->reserve(1);
        serialization.deserializeBinary(*new_value, buf, {});
        value = std::move(new_value);
    }
}

bool SingleValueDataGenericWithColumn::isEqualTo(const IColumn & column, size_t row_num) const
{
    return has() && !column.compareAt(row_num, 0, *value, -1);
}

bool SingleValueDataGenericWithColumn::isEqualTo(const SingleValueDataBase & other) const
{
    auto const & to = assert_cast<const Self &>(other);
    return has() && to.has() && !to.value->compareAt(0, 0, *value, -1);
}

void SingleValueDataGenericWithColumn::set(const IColumn & column, size_t row_num, Arena *)
{
    auto new_value = column.cloneEmpty();
    new_value->reserve(1);
    new_value->insertFrom(column, row_num);
    value = removeSpecialRepresentations(std::move(new_value));
}

void SingleValueDataGenericWithColumn::set(const SingleValueDataBase & other, Arena *)
{
    auto const & to = assert_cast<const Self &>(other);
    if (other.has())
        value = to.value;
}

bool SingleValueDataGenericWithColumn::setIfSmaller(const IColumn & column, size_t row_num, Arena * arena)
{
    if (!has())
    {
        set(column, row_num, arena);
        return true;
    }

    if (column.compareAt(row_num, 0, *value, -1) < 0)
    {
        set(column, row_num, arena);
        return true;
    }
    return false;
}

bool SingleValueDataGenericWithColumn::setIfSmaller(const SingleValueDataBase & other, Arena *)
{
    auto const & to = assert_cast<const Self &>(other);
    if (to.has() && (!has() || to.value->compareAt(0, 0, *value, -1) < 0))
    {
        value = to.value;
        return true;
    }
    return false;
}

bool SingleValueDataGenericWithColumn::setIfGreater(const IColumn & column, size_t row_num, Arena * arena)
{
    if (!has())
    {
        set(column, row_num, arena);
        return true;
    }

    if (column.compareAt(row_num, 0, *value, -1) > 0)
    {
        set(column, row_num, arena);
        return true;
    }
    return false;
}

bool SingleValueDataGenericWithColumn::setIfGreater(const SingleValueDataBase & other, Arena *)
{
    auto const & to = assert_cast<const Self &>(other);
    if (to.has() && (!has() || to.value->compareAt(0, 0, *value, -1) > 0))
    {
        value = to.value;
        return true;
    }
    return false;
}

void SingleValueReference::insertResultInto(DB::IColumn & to, const DataTypePtr &) const
{
    if (has())
        to.insertFrom(*column_ref, row_number);
    else
        assert_cast<ColumnString &>(to).insertDefault();
}

void SingleValueReference::write(WriteBuffer & /*buf*/, const ISerialization & /*serialization*/) const
{
    /// Not support
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "SingleValueReference::write is not implemented");
}

void SingleValueReference::read(ReadBuffer & /*buf*/, const ISerialization & /*serialization*/, const DataTypePtr & /*type*/, Arena * /*arena*/)
{
    /// Not support
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "SingleValueReference::read is not implemented");
}

bool SingleValueReference::isEqualTo(const DB::IColumn & column, size_t row_num) const
{
    return has()
        && column_ref->compareAt(row_number, row_num, column, -1) == 0;
}

bool SingleValueReference::isEqualTo(const SingleValueDataBase & /*other*/) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "SingleValueReference::isEqualTo is not implemented");
}

void SingleValueReference::set(const IColumn & column, size_t row_num, Arena * /*arena*/)
{
    column_ref.reset();
    column_ref = column.getPtr();
    row_number = row_num;
}

void SingleValueReference::set(const SingleValueDataBase & /*other*/, Arena * /*arena*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "SingleValueDataString::set is not implemented");
}

bool SingleValueReference::setIfSmaller(const IColumn & column, size_t row_num, Arena * arena)
{
    if (!has()
        || column_ref->compareAt(row_number, row_num, column, -1) > 0)
    {
        set(column, row_num, arena);
        return true;
    }
    return false;
}

bool SingleValueReference::setIfSmaller(const SingleValueDataBase & /*other*/, Arena * /*arena*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "SingleValueReference::setIfSmaller is not implemented");
}


bool SingleValueReference::setIfGreater(const IColumn & column, size_t row_num, Arena * arena)
{
    if (!has()
        || column_ref->compareAt(row_number, row_num, column, -1) < 0)
    {
        set(column, row_num, arena);
        return true;
    }
    return false;
}

bool SingleValueReference::setIfGreater(const SingleValueDataBase & /*other*/, Arena * /*arena*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "SingleValueReference::setIfGreater is not implemented");
}

bool canUseFieldForValueData(const DataTypePtr & value_type)
{
    bool result = true;
    auto check = [&](const IDataType & type)
    {
        /// Variant, Dynamic and Object types doesn't work well with Field
        /// because they can store values of different data types in a single column.
        result &= !isVariant(type) && !isDynamic(type) && !isObject(type);
    };

    check(*value_type);
    value_type->forEachChild(check);
    return result;
};

void generateSingleValueFromType(const DataTypePtr & type, SingleValueDataBaseMemoryBlock & data)
{
    auto idx = type->getTypeId();
#define DISPATCH(TYPE) \
    if (idx == TypeIndex::TYPE) \
    { \
        static_assert(sizeof(SingleValueDataNumeric<TYPE>) <= sizeof(SingleValueDataBaseMemoryBlock::memory)); \
        static_assert(alignof(SingleValueDataNumeric<TYPE>) <= alignof(SingleValueDataBaseMemoryBlock)); \
        new (&data.memory) SingleValueDataNumeric<TYPE>; \
        return; \
    }

    FOR_SINGLE_VALUE_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH

    if (idx == TypeIndex::Date)
    {
        static_assert(sizeof(SingleValueDataNumeric<DataTypeDate::FieldType>) <= sizeof(SingleValueDataBaseMemoryBlock::memory));
        static_assert(alignof(SingleValueDataNumeric<DataTypeDate::FieldType>) <= alignof(SingleValueDataBaseMemoryBlock));
        new (&data.memory) SingleValueDataNumeric<DataTypeDate::FieldType>;
        return;
    }
    if (idx == TypeIndex::DateTime)
    {
        static_assert(sizeof(SingleValueDataNumeric<DataTypeDateTime::FieldType>) <= sizeof(SingleValueDataBaseMemoryBlock::memory));
        static_assert(alignof(SingleValueDataNumeric<DataTypeDateTime::FieldType>) <= alignof(SingleValueDataBaseMemoryBlock));
        new (&data.memory) SingleValueDataNumeric<DataTypeDateTime::FieldType>;
        return;
    }
    if (idx == TypeIndex::String)
    {
        static_assert(sizeof(SingleValueDataString) <= sizeof(SingleValueDataBaseMemoryBlock::memory));
        static_assert(alignof(SingleValueDataString) <= alignof(SingleValueDataBaseMemoryBlock));
        new (&data.memory) SingleValueDataString;
        return;
    }

    if (canUseFieldForValueData(type))
    {
        static_assert(sizeof(SingleValueDataGeneric) <= sizeof(SingleValueDataBaseMemoryBlock::memory));
        static_assert(alignof(SingleValueDataGeneric) <= alignof(SingleValueDataBaseMemoryBlock));
        new (&data.memory) SingleValueDataGeneric;
        return;
    }

    static_assert(sizeof(SingleValueDataGenericWithColumn) <= sizeof(SingleValueDataBaseMemoryBlock::memory));
    static_assert(alignof(SingleValueDataGenericWithColumn) <= alignof(SingleValueDataBaseMemoryBlock));
    new (&data.memory) SingleValueDataGenericWithColumn;
}

bool singleValueTypeAllocatesMemoryInArena(TypeIndex idx)
{
    return idx == TypeIndex::String;
}
}
