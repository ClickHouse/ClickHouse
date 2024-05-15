#pragma once

#include <array>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnNullable.h>
#include <Common/assert_cast.h>
#include <Columns/ColumnsCommon.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include "config.h"

#if USE_EMBEDDED_COMPILER
#    include <llvm/IR/IRBuilder.h>
#    include <DataTypes/Native.h>
#endif

namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


/// This class implements a wrapper around an aggregate function. Despite its name,
/// this is an adapter. It is used to handle aggregate functions that are called with
/// at least one nullable argument. It implements the logic according to which any
/// row that contains at least one NULL is skipped.

/// If all rows had NULL, the behaviour is determined by "result_is_nullable" template parameter.
///  true - return NULL; false - return value from empty aggregation state of nested function.

/// When serialize_flag is set to true, the flag about presence of values is serialized
///  regardless to the "result_is_nullable" even if it's unneeded - for protocol compatibility.

template <bool result_is_nullable, bool serialize_flag, typename Derived>
class AggregateFunctionNullBase : public IAggregateFunctionHelper<Derived>
{
protected:
    const AggregateFunctionPtr nested_function;
    const size_t prefix_size;

    /** In addition to data for nested aggregate function, we keep a flag
      *  indicating - was there at least one non-NULL value accumulated.
      * In case of no not-NULL values, the function will return NULL.
      *
      * We use prefix_size bytes for flag to satisfy the alignment requirement of nested state.
      */

    AggregateDataPtr nestedPlace(AggregateDataPtr __restrict place) const noexcept
    {
        if constexpr (result_is_nullable)
            return place + prefix_size;
        else
            return place;
    }

    ConstAggregateDataPtr nestedPlace(ConstAggregateDataPtr __restrict place) const noexcept
    {
        if constexpr (result_is_nullable)
            return place + prefix_size;
        else
            return place;
    }

    static void initFlag(AggregateDataPtr __restrict place) noexcept
    {
        if constexpr (result_is_nullable)
            place[0] = 0;
    }

    static void setFlag(AggregateDataPtr __restrict place) noexcept
    {
        if constexpr (result_is_nullable)
            place[0] = 1;
    }

    static bool getFlag(ConstAggregateDataPtr __restrict place) noexcept
    {
        if constexpr (result_is_nullable)
            return place[0];
        else
            return true;
    }

public:
    AggregateFunctionNullBase(AggregateFunctionPtr nested_function_, const DataTypes & arguments, const Array & params)
        : IAggregateFunctionHelper<Derived>(arguments, params, createResultType(nested_function_))
        , nested_function{nested_function_}
        , prefix_size(result_is_nullable ? nested_function->alignOfData() : 0)
    {
    }

    String getName() const override
    {
        /// This is just a wrapper. The function for Nullable arguments is named the same as the nested function itself.
        return nested_function->getName();
    }

    static DataTypePtr createResultType(const AggregateFunctionPtr & nested_function_)
    {
        if constexpr (result_is_nullable)
            return makeNullable(nested_function_->getResultType());
        else
            return nested_function_->getResultType();
    }

    void create(AggregateDataPtr __restrict place) const override
    {
        initFlag(place);
        nested_function->create(nestedPlace(place));
    }

    void destroy(AggregateDataPtr __restrict place) const noexcept override
    {
        nested_function->destroy(nestedPlace(place));
    }

    void destroyUpToState(AggregateDataPtr __restrict place) const noexcept override
    {
        nested_function->destroyUpToState(nestedPlace(place));
    }

    bool hasTrivialDestructor() const override
    {
        return nested_function->hasTrivialDestructor();
    }

    size_t sizeOfData() const override
    {
        return prefix_size + nested_function->sizeOfData();
    }

    size_t alignOfData() const override
    {
        return nested_function->alignOfData();
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        if constexpr (result_is_nullable)
            if (getFlag(rhs))
                setFlag(place);

        nested_function->merge(nestedPlace(place), nestedPlace(rhs), arena);
    }

    bool isAbleToParallelizeMerge() const override { return nested_function->isAbleToParallelizeMerge(); }
    bool canOptimizeEqualKeysRanges() const override { return nested_function->canOptimizeEqualKeysRanges(); }

    void parallelizeMergePrepare(AggregateDataPtrs & places, ThreadPool & thread_pool, std::atomic<bool> & is_cancelled) const override
    {
        AggregateDataPtrs nested_places(places.begin(), places.end());
        for (auto & nested_place : nested_places)
            nested_place = nestedPlace(nested_place);

        nested_function->parallelizeMergePrepare(nested_places, thread_pool, is_cancelled);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, ThreadPool & thread_pool, std::atomic<bool> & is_cancelled, Arena * arena) const override
    {
        nested_function->merge(nestedPlace(place), nestedPlace(rhs), thread_pool, is_cancelled, arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> version) const override
    {
        bool flag = getFlag(place);
        if constexpr (serialize_flag)
            writeBinary(flag, buf);
        if (flag)
            nested_function->serialize(nestedPlace(place), buf, version);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> version, Arena * arena) const override
    {
        bool flag = true;
        if constexpr (serialize_flag)
            readBinary(flag, buf);
        if (flag)
        {
            setFlag(place);
            nested_function->deserialize(nestedPlace(place), buf, version, arena);
        }
    }

    template <bool merge>
    void insertResultIntoImpl(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const
    {
        if constexpr (result_is_nullable)
        {
            ColumnNullable & to_concrete = assert_cast<ColumnNullable &>(to);
            if (getFlag(place))
            {
                if constexpr (merge)
                    nested_function->insertMergeResultInto(nestedPlace(place), to_concrete.getNestedColumn(), arena);
                else
                    nested_function->insertResultInto(nestedPlace(place), to_concrete.getNestedColumn(), arena);
                to_concrete.getNullMapData().push_back(0);
            }
            else
            {
                to_concrete.insertDefault();
            }
        }
        else
        {
            if constexpr (merge)
                nested_function->insertMergeResultInto(nestedPlace(place), to, arena);
            else
                nested_function->insertResultInto(nestedPlace(place), to, arena);
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        insertResultIntoImpl<false>(place, to, arena);
    }

    void insertMergeResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        insertResultIntoImpl<true>(place, to, arena);
    }

    bool allocatesMemoryInArena() const override
    {
        return nested_function->allocatesMemoryInArena();
    }

    bool isState() const override
    {
        return nested_function->isState();
    }

    bool isVersioned() const override
    {
        return nested_function->isVersioned();
    }

    size_t getVersionFromRevision(size_t revision) const override
    {
        return nested_function->getVersionFromRevision(revision);
    }

    size_t getDefaultVersion() const override
    {
        return nested_function->getDefaultVersion();
    }

    AggregateFunctionPtr getNestedFunction() const override { return nested_function; }

#if USE_EMBEDDED_COMPILER

    bool isCompilable() const override
    {
        return this->nested_function->isCompilable();
    }

    void compileCreate(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr) const override
    {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        if constexpr (result_is_nullable)
            b.CreateMemSet(aggregate_data_ptr, llvm::ConstantInt::get(b.getInt8Ty(), 0), this->prefix_size, llvm::assumeAligned(this->alignOfData()));

        auto * aggregate_data_ptr_with_prefix_size_offset = b.CreateConstInBoundsGEP1_64(b.getInt8Ty(), aggregate_data_ptr, this->prefix_size);
        this->nested_function->compileCreate(b, aggregate_data_ptr_with_prefix_size_offset);
    }

    void compileMerge(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_dst_ptr, llvm::Value * aggregate_data_src_ptr) const override
    {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        if constexpr (result_is_nullable)
        {
            auto * aggregate_data_is_null_dst_value = b.CreateLoad(b.getInt8Ty(), aggregate_data_dst_ptr);
            auto * aggregate_data_is_null_src_value = b.CreateLoad(b.getInt8Ty(), aggregate_data_src_ptr);

            auto * is_src_null = nativeBoolCast(b, std::make_shared<DataTypeUInt8>(), aggregate_data_is_null_src_value);
            auto * is_null_result_value = b.CreateSelect(is_src_null, llvm::ConstantInt::get(b.getInt8Ty(), 1), aggregate_data_is_null_dst_value);
            b.CreateStore(is_null_result_value, aggregate_data_dst_ptr);
        }

        auto * aggregate_data_dst_ptr_with_prefix_size_offset = b.CreateConstInBoundsGEP1_64(b.getInt8Ty(), aggregate_data_dst_ptr, this->prefix_size);
        auto * aggregate_data_src_ptr_with_prefix_size_offset = b.CreateConstInBoundsGEP1_64(b.getInt8Ty(), aggregate_data_src_ptr, this->prefix_size);

        this->nested_function->compileMerge(b, aggregate_data_dst_ptr_with_prefix_size_offset, aggregate_data_src_ptr_with_prefix_size_offset);
    }

    llvm::Value * compileGetResult(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr) const override
    {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        auto * return_type = toNativeType(b, this->getResultType());

        llvm::Value * result = nullptr;

        if constexpr (result_is_nullable)
        {
            auto * place = b.CreateLoad(b.getInt8Ty(), aggregate_data_ptr);

            auto * head = b.GetInsertBlock();

            auto * join_block = llvm::BasicBlock::Create(head->getContext(), "join_block", head->getParent());
            auto * if_null = llvm::BasicBlock::Create(head->getContext(), "if_null", head->getParent());
            auto * if_not_null = llvm::BasicBlock::Create(head->getContext(), "if_not_null", head->getParent());

            auto * nullable_value_ptr = b.CreateAlloca(return_type);
            b.CreateStore(llvm::ConstantInt::getNullValue(return_type), nullable_value_ptr);
            auto * nullable_value = b.CreateLoad(return_type, nullable_value_ptr);

            b.CreateCondBr(nativeBoolCast(b, std::make_shared<DataTypeUInt8>(), place), if_not_null, if_null);

            b.SetInsertPoint(if_null);
            b.CreateStore(b.CreateInsertValue(nullable_value, b.getInt1(true), {1}), nullable_value_ptr);
            b.CreateBr(join_block);

            b.SetInsertPoint(if_not_null);
            auto * aggregate_data_ptr_with_prefix_size_offset = b.CreateConstInBoundsGEP1_64(b.getInt8Ty(), aggregate_data_ptr, this->prefix_size);
            auto * nested_result = this->nested_function->compileGetResult(builder, aggregate_data_ptr_with_prefix_size_offset);
            b.CreateStore(b.CreateInsertValue(nullable_value, nested_result, {0}), nullable_value_ptr);
            b.CreateBr(join_block);

            b.SetInsertPoint(join_block);

            result = b.CreateLoad(return_type, nullable_value_ptr);
        }
        else
        {
            result = this->nested_function->compileGetResult(b, aggregate_data_ptr);
        }

        return result;
    }

#endif

};


/** There are two cases: for single argument and variadic.
  * Code for single argument is much more efficient.
  */
template <bool result_is_nullable, bool serialize_flag>
class AggregateFunctionNullUnary final
    : public AggregateFunctionNullBase<result_is_nullable, serialize_flag,
        AggregateFunctionNullUnary<result_is_nullable, serialize_flag>>
{
public:
    AggregateFunctionNullUnary(AggregateFunctionPtr nested_function_, const DataTypes & arguments, const Array & params)
        : AggregateFunctionNullBase<result_is_nullable, serialize_flag,
            AggregateFunctionNullUnary<result_is_nullable, serialize_flag>>(std::move(nested_function_), arguments, params)
    {
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        const ColumnNullable * column = assert_cast<const ColumnNullable *>(columns[0]);
        const IColumn * nested_column = &column->getNestedColumn();
        if (!column->isNullAt(row_num))
        {
            this->setFlag(place);
            this->nested_function->add(this->nestedPlace(place), &nested_column, row_num, arena);
        }
    }

    void addBatchSinglePlace( /// NOLINT
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        Arena * arena,
        ssize_t if_argument_pos = -1) const override
    {
        const ColumnNullable * column = assert_cast<const ColumnNullable *>(columns[0]);
        const IColumn * nested_column = &column->getNestedColumn();
        const UInt8 * null_map = column->getNullMapData().data();

        this->nested_function->addBatchSinglePlaceNotNull(
            row_begin, row_end, this->nestedPlace(place), &nested_column, null_map, arena, if_argument_pos);

        if constexpr (result_is_nullable)
            if (!memoryIsByte(null_map, row_begin, row_end, 1))
                this->setFlag(place);
    }

#if USE_EMBEDDED_COMPILER

    void compileAdd(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr, const ValuesWithType & arguments) const override
    {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        const auto & nullable_type = arguments[0].type;
        const auto & nullable_value = arguments[0].value;

        auto * wrapped_value = b.CreateExtractValue(nullable_value, {0});
        auto * is_null_value = b.CreateExtractValue(nullable_value, {1});

        auto * head = b.GetInsertBlock();

        auto * join_block = llvm::BasicBlock::Create(head->getContext(), "join_block", head->getParent());
        auto * if_null = llvm::BasicBlock::Create(head->getContext(), "if_null", head->getParent());
        auto * if_not_null = llvm::BasicBlock::Create(head->getContext(), "if_not_null", head->getParent());

        b.CreateCondBr(is_null_value, if_null, if_not_null);

        b.SetInsertPoint(if_null);
        b.CreateBr(join_block);

        b.SetInsertPoint(if_not_null);

        if constexpr (result_is_nullable)
            b.CreateStore(llvm::ConstantInt::get(b.getInt8Ty(), 1), aggregate_data_ptr);

        auto * aggregate_data_ptr_with_prefix_size_offset = b.CreateConstInBoundsGEP1_64(b.getInt8Ty(), aggregate_data_ptr, this->prefix_size);
        this->nested_function->compileAdd(b, aggregate_data_ptr_with_prefix_size_offset, { ValueWithType(wrapped_value, removeNullable(nullable_type)) });
        b.CreateBr(join_block);

        b.SetInsertPoint(join_block);
    }

#endif

};


template <bool result_is_nullable, bool serialize_flag>
class AggregateFunctionNullVariadic final : public AggregateFunctionNullBase<
                                                result_is_nullable,
                                                serialize_flag,
                                                AggregateFunctionNullVariadic<result_is_nullable, serialize_flag>>
{
public:
    AggregateFunctionNullVariadic(AggregateFunctionPtr nested_function_, const DataTypes & arguments, const Array & params)
        : AggregateFunctionNullBase<result_is_nullable, serialize_flag, AggregateFunctionNullVariadic<result_is_nullable, serialize_flag>>(
            std::move(nested_function_), arguments, params)
        , number_of_arguments(arguments.size())
    {
        if (number_of_arguments == 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Single argument is passed to AggregateFunctionNullVariadic");

        if (number_of_arguments > MAX_ARGS)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Maximum number of arguments for aggregate function with Nullable types is {}",
                size_t(MAX_ARGS));

        for (size_t i = 0; i < number_of_arguments; ++i)
            is_nullable[i] = arguments[i]->isNullable();
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        /// This container stores the columns we really pass to the nested function.
        const IColumn * nested_columns[number_of_arguments];

        for (size_t i = 0; i < number_of_arguments; ++i)
        {
            if (is_nullable[i])
            {
                const ColumnNullable & nullable_col = assert_cast<const ColumnNullable &>(*columns[i]);
                if (nullable_col.isNullAt(row_num))
                {
                    /// If at least one column has a null value in the current row,
                    /// we don't process this row.
                    return;
                }
                nested_columns[i] = &nullable_col.getNestedColumn();
            }
            else
                nested_columns[i] = columns[i];
        }

        this->setFlag(place);
        this->nested_function->add(this->nestedPlace(place), nested_columns, row_num, arena);
    }

    void addBatchSinglePlace(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        Arena * arena,
        ssize_t if_argument_pos) const final
    {
        /// We are going to merge all the flags into a single one to be able to call the nested batching functions
        std::vector<const UInt8 *> nullable_filters;
        const IColumn * nested_columns[number_of_arguments];

        std::unique_ptr<UInt8[]> final_flags;
        const UInt8 * final_flags_ptr = nullptr;

        if (if_argument_pos >= 0)
        {
            final_flags = std::make_unique<UInt8[]>(row_end);
            final_flags_ptr = final_flags.get();

            size_t included_elements = 0;
            const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            for (size_t i = row_begin; i < row_end; i++)
            {
                final_flags[i] = !flags.data()[i];
                included_elements += !!flags.data()[i];
            }

            if (included_elements == 0)
                return;
            if (included_elements != (row_end - row_begin))
            {
                nullable_filters.push_back(final_flags_ptr);
            }
        }

        for (size_t i = 0; i < number_of_arguments; ++i)
        {
            if (is_nullable[i])
            {
                const ColumnNullable & nullable_col = assert_cast<const ColumnNullable &>(*columns[i]);
                nested_columns[i] = &nullable_col.getNestedColumn();
                const ColumnUInt8 & nullmap_column = nullable_col.getNullMapColumn();
                nullable_filters.push_back(nullmap_column.getData().data());
            }
            else
            {
                nested_columns[i] = columns[i];
            }
        }

        chassert(!nullable_filters.empty());
        bool found_one = false;
        if (nullable_filters.size() == 1)
        {
            /// We can avoid making copies of the only filter but we still need to check that there is data to be added
            final_flags_ptr = nullable_filters[0];
            for (size_t i = row_begin; i < row_end; i++)
            {
                if (!final_flags_ptr[i])
                {
                    found_one = true;
                    break;
                }
            }
        }
        else
        {
            if (!final_flags)
            {
                final_flags = std::make_unique<UInt8[]>(row_end);
                final_flags_ptr = final_flags.get();
            }

            const size_t filter_start = nullable_filters[0] == final_flags_ptr ? 1 : 0;
            for (size_t filter = filter_start; filter < nullable_filters.size(); filter++)
            {
                for (size_t i = row_begin; i < row_end; i++)
                    final_flags[i] |= nullable_filters[filter][i];
            }

            for (size_t i = row_begin; i < row_end; i++)
            {
                if (!final_flags_ptr[i])
                {
                    found_one = true;
                    break;
                }
            }
        }

        if (!found_one)
            return; // Nothing to do and nothing to mark

        this->setFlag(place);
        this->nested_function->addBatchSinglePlaceNotNull(
            row_begin, row_end, this->nestedPlace(place), nested_columns, final_flags_ptr, arena, -1);
    }


#if USE_EMBEDDED_COMPILER

    void compileAdd(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr, const ValuesWithType & arguments) const override
    {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        size_t arguments_size = arguments.size();

        ValuesWithType wrapped_arguments;
        wrapped_arguments.reserve(arguments_size);

        std::vector<llvm::Value *> is_null_values;
        is_null_values.reserve(arguments_size);

        for (size_t i = 0; i < arguments_size; ++i)
        {
            const auto & argument_value = arguments[i].value;
            const auto & argument_type = arguments[i].type;

            if (is_nullable[i])
            {
                auto * wrapped_value = b.CreateExtractValue(argument_value, {0});
                is_null_values.emplace_back(b.CreateExtractValue(argument_value, {1}));
                wrapped_arguments.emplace_back(wrapped_value, removeNullable(argument_type));
            }
            else
            {
                wrapped_arguments.emplace_back(argument_value, argument_type);
            }
        }

        auto * head = b.GetInsertBlock();

        auto * join_block = llvm::BasicBlock::Create(head->getContext(), "join_block", head->getParent());
        auto * if_null = llvm::BasicBlock::Create(head->getContext(), "if_null", head->getParent());
        auto * if_not_null = llvm::BasicBlock::Create(head->getContext(), "if_not_null", head->getParent());

        auto * values_have_null_ptr = b.CreateAlloca(b.getInt1Ty());
        b.CreateStore(b.getInt1(false), values_have_null_ptr);

        for (auto * is_null_value : is_null_values)
        {
            auto * values_have_null = b.CreateLoad(b.getInt1Ty(), values_have_null_ptr);
            b.CreateStore(b.CreateOr(values_have_null, is_null_value), values_have_null_ptr);
        }

        b.CreateCondBr(b.CreateLoad(b.getInt1Ty(), values_have_null_ptr), if_null, if_not_null);

        b.SetInsertPoint(if_null);
        b.CreateBr(join_block);

        b.SetInsertPoint(if_not_null);

        if constexpr (result_is_nullable)
            b.CreateStore(llvm::ConstantInt::get(b.getInt8Ty(), 1), aggregate_data_ptr);

        auto * aggregate_data_ptr_with_prefix_size_offset = b.CreateConstInBoundsGEP1_64(b.getInt8Ty(), aggregate_data_ptr, this->prefix_size);
        this->nested_function->compileAdd(b, aggregate_data_ptr_with_prefix_size_offset, wrapped_arguments);
        b.CreateBr(join_block);

        b.SetInsertPoint(join_block);
    }

#endif

private:
    enum { MAX_ARGS = 8 };
    size_t number_of_arguments = 0;
    std::array<char, MAX_ARGS> is_nullable;    /// Plain array is better than std::vector due to one indirection less.
};

}
