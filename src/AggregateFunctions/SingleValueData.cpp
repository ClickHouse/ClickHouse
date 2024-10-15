#include <AggregateFunctions/SingleValueData.h>
#include <Columns/ColumnString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/Arena.h>
#include <Common/assert_cast.h>
#include <Common/findExtreme.h>

#if USE_EMBEDDED_COMPILER
#    include <DataTypes/Native.h>
#    include <llvm/IR/IRBuilder.h>
#endif

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int TOO_LARGE_STRING_SIZE;
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
void SingleValueDataFixed<T>::insertResultInto(IColumn & to) const
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
void SingleValueDataFixed<T>::read(ReadBuffer & buf, const ISerialization &, Arena *)
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
    if constexpr (has_find_extreme_implementation<T>)
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
    if constexpr (has_find_extreme_implementation<T>)
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

    if constexpr (has_find_extreme_implementation<T>)
    {
        std::optional<T> opt;
        if (!if_map)
        {
            opt = findExtremeMinNotNull(vec.getData().data(), null_map, row_begin, row_end);
            if (!opt.has_value())
                return opt;
            for (size_t i = row_begin; i < row_end; i++)
            {
                if (!null_map[i] && vec[i] == *opt)
                    return {i};
            }
        }
        else if (!null_map)
        {
            opt = findExtremeMinIf(vec.getData().data(), if_map, row_begin, row_end);
            if (!opt.has_value())
                return opt;
            for (size_t i = row_begin; i < row_end; i++)
            {
                if (if_map[i] && vec[i] == *opt)
                    return {i};
            }
        }
        else
        {
            auto final_flags = mergeIfAndNullFlags(null_map, if_map, row_begin, row_end);
            opt = findExtremeMinIf(vec.getData().data(), final_flags.get(), row_begin, row_end);
            if (!opt.has_value())
                return std::nullopt;
            for (size_t i = row_begin; i < row_end; i++)
            {
                if (final_flags[i] && vec[i] == *opt)
                    return {i};
            }
        }
        UNREACHABLE();
    }
    else
    {
        size_t index = row_begin;
        while ((index < row_end) && ((if_map && if_map[index] == 0) || (null_map && null_map[index] != 0)))
            index++;
        if (index >= row_end)
            return std::nullopt;

        for (size_t i = index + 1; i < row_end; i++)
            if ((!if_map || if_map[i] != 0) && (!null_map || null_map[i] == 0) && (vec[i] < vec[index]))
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

    if constexpr (has_find_extreme_implementation<T>)
    {
        std::optional<T> opt;
        if (!if_map)
        {
            opt = findExtremeMaxNotNull(vec.getData().data(), null_map, row_begin, row_end);
            if (!opt.has_value())
                return opt;
            for (size_t i = row_begin; i < row_end; i++)
            {
                if (!null_map[i] && vec[i] == *opt)
                    return {i};
            }
            return opt;
        }
        if (!null_map)
        {
            opt = findExtremeMaxIf(vec.getData().data(), if_map, row_begin, row_end);
            if (!opt.has_value())
                return opt;
            for (size_t i = row_begin; i < row_end; i++)
            {
                if (if_map[i] && vec[i] == *opt)
                    return {i};
            }
            return opt;
        }

        auto final_flags = mergeIfAndNullFlags(null_map, if_map, row_begin, row_end);
        opt = findExtremeMaxIf(vec.getData().data(), final_flags.get(), row_begin, row_end);
        if (!opt.has_value())
            return std::nullopt;
        for (size_t i = row_begin; i < row_end; i++)
        {
            if (final_flags[i] && vec[i] == *opt)
                return {i};
        }

        UNREACHABLE();
    }
    else
    {
        size_t index = row_begin;
        while ((index < row_end) && ((if_map && if_map[index] == 0) || (null_map && null_map[index] != 0)))
            index++;
        if (index >= row_end)
            return std::nullopt;

        for (size_t i = index + 1; i < row_end; i++)
            if ((!if_map || if_map[i] != 0) && (!null_map || null_map[i] == 0) && (vec[i] > vec[index]))
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
void SingleValueDataNumeric<T>::insertResultInto(IColumn & to) const
{
    return memory.get().insertResultInto(to);
}

template <typename T>
void SingleValueDataNumeric<T>::write(DB::WriteBuffer & buf, const DB::ISerialization & serialization) const
{
    return memory.get().write(buf, serialization);
}

template <typename T>
void SingleValueDataNumeric<T>::read(DB::ReadBuffer & buf, const DB::ISerialization & serialization, DB::Arena * arena)
{
    return memory.get().read(buf, serialization, arena);
}

template <typename T>
bool SingleValueDataNumeric<T>::isEqualTo(const DB::IColumn & column, size_t index) const
{
    return memory.get().isEqualTo(column, index);
}

template <typename T>
bool SingleValueDataNumeric<T>::isEqualTo(const DB::SingleValueDataBase & to) const
{
    /// to.has() is checked in memory.get().isEqualTo
    auto const & other = assert_cast<const Self &>(to);
    return memory.get().isEqualTo(other.memory.get());
}

template <typename T>
void SingleValueDataNumeric<T>::set(const DB::IColumn & column, size_t row_num, DB::Arena * arena)
{
    return memory.get().set(column, row_num, arena);
}

template <typename T>
void SingleValueDataNumeric<T>::set(const DB::SingleValueDataBase & to, DB::Arena * arena)
{
    /// to.has() is checked in memory.get().set
    auto const & other = assert_cast<const Self &>(to);
    return memory.get().set(other.memory.get(), arena);
}

template <typename T>
bool SingleValueDataNumeric<T>::setIfSmaller(const DB::SingleValueDataBase & to, DB::Arena * arena)
{
    /// to.has() is checked in memory.get().setIfSmaller
    auto const & other = assert_cast<const Self &>(to);
    return memory.get().setIfSmaller(other.memory.get(), arena);
}

template <typename T>
bool SingleValueDataNumeric<T>::setIfGreater(const DB::SingleValueDataBase & to, DB::Arena * arena)
{
    /// to.has() is checked in memory.get().setIfGreater
    auto const & other = assert_cast<const Self &>(to);
    return memory.get().setIfGreater(other.memory.get(), arena);
}

template <typename T>
bool SingleValueDataNumeric<T>::setIfSmaller(const DB::IColumn & column, size_t row_num, DB::Arena * arena)
{
    return memory.get().setIfSmaller(column, row_num, arena);
}

template <typename T>
bool SingleValueDataNumeric<T>::setIfGreater(const DB::IColumn & column, size_t row_num, DB::Arena * arena)
{
    return memory.get().setIfGreater(column, row_num, arena);
}

template <typename T>
void SingleValueDataNumeric<T>::setSmallest(const DB::IColumn & column, size_t row_begin, size_t row_end, DB::Arena * arena)
{
    return memory.get().setSmallest(column, row_begin, row_end, arena);
}

template <typename T>
void SingleValueDataNumeric<T>::setGreatest(const DB::IColumn & column, size_t row_begin, size_t row_end, DB::Arena * arena)
{
    return memory.get().setGreatest(column, row_begin, row_end, arena);
}

template <typename T>
void SingleValueDataNumeric<T>::setSmallestNotNullIf(
    const DB::IColumn & column,
    const UInt8 * __restrict null_map,
    const UInt8 * __restrict if_map,
    size_t row_begin,
    size_t row_end,
    DB::Arena * arena)
{
    return memory.get().setSmallestNotNullIf(column, null_map, if_map, row_begin, row_end, arena);
}

template <typename T>
void SingleValueDataNumeric<T>::setGreatestNotNullIf(
    const DB::IColumn & column,
    const UInt8 * __restrict null_map,
    const UInt8 * __restrict if_map,
    size_t row_begin,
    size_t row_end,
    DB::Arena * arena)
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

char * SingleValueDataString::getDataMutable()
{
    return size <= MAX_SMALL_STRING_SIZE ? small_data : large_data;
}

const char * SingleValueDataString::getData() const
{
    const char * data_ptr = size <= MAX_SMALL_STRING_SIZE ? small_data : large_data;
    /// It must always be terminated with null-character
    chassert(0 < size);
    chassert(data_ptr[size - 1] == '\0');
    return data_ptr;
}

StringRef SingleValueDataString::getStringRef() const
{
    return StringRef(getData(), size);
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

    if (value_size <= MAX_SMALL_STRING_SIZE)
    {
        /// Don't free large_data here.
        size = value_size;

        if (size > 0)
            memcpy(small_data, value.data, size);
    }
    else
    {
        allocateLargeDataIfNeeded(value_size, arena);
        size = value_size;
        memcpy(large_data, value.data, size);
    }
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
    return has() && to.has() && to.getStringRef() == getStringRef();
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
    if (!has()
        || StringValueCompatibility::getDataAtWithTerminatingZero(assert_cast<const ColumnString &>(column), row_num) > getStringRef())
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

void SingleValueDataGeneric::insertResultInto(IColumn & to) const
{
    if (has())
        to.insert(value);
    else
        to.insertDefault();
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

void SingleValueDataGeneric::read(ReadBuffer & buf, const ISerialization & serialization, Arena *)
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

bool SingleValueDataGeneric::isEqualTo(const DB::SingleValueDataBase & other) const
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

void generateSingleValueFromTypeIndex(TypeIndex idx, SingleValueDataBaseMemoryBlock & data)
{
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
    static_assert(sizeof(SingleValueDataGeneric) <= sizeof(SingleValueDataBaseMemoryBlock::memory));
    static_assert(alignof(SingleValueDataGeneric) <= alignof(SingleValueDataBaseMemoryBlock));
    new (&data.memory) SingleValueDataGeneric;
}

bool singleValueTypeAllocatesMemoryInArena(TypeIndex idx)
{
    return idx == TypeIndex::String;
}
}
