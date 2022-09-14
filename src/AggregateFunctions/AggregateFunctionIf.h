#pragma once

#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include <Common/assert_cast.h>
#include <AggregateFunctions/IAggregateFunction.h>

#include <Common/config.h>

#if USE_EMBEDDED_COMPILER
#    include <llvm/IR/IRBuilder.h>
#    include <DataTypes/Native.h>
#endif

namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

/** Not an aggregate function, but an adapter of aggregate functions,
  * which any aggregate function `agg(x)` makes an aggregate function of the form `aggIf(x, cond)`.
  * The adapted aggregate function takes two arguments - a value and a condition,
  * and calculates the nested aggregate function for the values when the condition is satisfied.
  * For example, avgIf(x, cond) calculates the average x if `cond`.
  */
class AggregateFunctionIf final : public IAggregateFunctionHelper<AggregateFunctionIf>
{
private:
    AggregateFunctionPtr nested_func;
    size_t num_arguments;

public:
    AggregateFunctionIf(AggregateFunctionPtr nested, const DataTypes & types, const Array & params_)
        : IAggregateFunctionHelper<AggregateFunctionIf>(types, params_)
        , nested_func(nested), num_arguments(types.size())
    {
        if (num_arguments == 0)
            throw Exception("Aggregate function " + getName() + " require at least one argument", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (!isUInt8(types.back()))
            throw Exception("Last argument for aggregate function " + getName() + " must be UInt8", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    String getName() const override
    {
        return nested_func->getName() + "If";
    }

    DataTypePtr getReturnType() const override
    {
        return nested_func->getReturnType();
    }

    bool isVersioned() const override
    {
        return nested_func->isVersioned();
    }

    size_t getDefaultVersion() const override
    {
        return nested_func->getDefaultVersion();
    }

    void create(AggregateDataPtr __restrict place) const override
    {
        nested_func->create(place);
    }

    void destroy(AggregateDataPtr __restrict place) const noexcept override
    {
        nested_func->destroy(place);
    }

    bool hasTrivialDestructor() const override
    {
        return nested_func->hasTrivialDestructor();
    }

    size_t sizeOfData() const override
    {
        return nested_func->sizeOfData();
    }

    size_t alignOfData() const override
    {
        return nested_func->alignOfData();
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        if (assert_cast<const ColumnUInt8 &>(*columns[num_arguments - 1]).getData()[row_num])
            nested_func->add(place, columns, row_num, arena);
    }

    void addBatch(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr * places,
        size_t place_offset,
        const IColumn ** columns,
        Arena * arena,
        ssize_t) const override
    {
        nested_func->addBatch(row_begin, row_end, places, place_offset, columns, arena, num_arguments - 1);
    }

    void addBatchSinglePlace(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr place,
        const IColumn ** columns,
        Arena * arena,
        ssize_t) const override
    {
        nested_func->addBatchSinglePlace(row_begin, row_end, place, columns, arena, num_arguments - 1);
    }

    void addBatchSinglePlaceNotNull(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr place,
        const IColumn ** columns,
        const UInt8 * null_map,
        Arena * arena,
        ssize_t) const override
    {
        nested_func->addBatchSinglePlaceNotNull(row_begin, row_end, place, columns, null_map, arena, num_arguments - 1);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        nested_func->merge(place, rhs, arena);
    }

    void mergeBatch(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr * places,
        size_t place_offset,
        const AggregateDataPtr * rhs,
        Arena * arena) const override
    {
        nested_func->mergeBatch(row_begin, row_end, places, place_offset, rhs, arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> version) const override
    {
        nested_func->serialize(place, buf, version);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> version, Arena * arena) const override
    {
        nested_func->deserialize(place, buf, version, arena);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        nested_func->insertResultInto(place, to, arena);
    }

    bool allocatesMemoryInArena() const override
    {
        return nested_func->allocatesMemoryInArena();
    }

    bool isState() const override
    {
        return nested_func->isState();
    }

    AggregateFunctionPtr getOwnNullAdapter(
        const AggregateFunctionPtr & nested_function, const DataTypes & arguments,
        const Array & params, const AggregateFunctionProperties & properties) const override;

    AggregateFunctionPtr getNestedFunction() const override { return nested_func; }


#if USE_EMBEDDED_COMPILER

    bool isCompilable() const override
    {
        return nested_func->isCompilable();
    }

    void compileCreate(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr) const override
    {
        nested_func->compileCreate(builder, aggregate_data_ptr);
    }

    void compileAdd(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr, const DataTypes & arguments_types, const std::vector<llvm::Value *> & argument_values) const override
    {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        const auto & predicate_type = arguments_types[argument_values.size() - 1];
        auto * predicate_value = argument_values[argument_values.size() - 1];

        auto * head = b.GetInsertBlock();

        auto * join_block = llvm::BasicBlock::Create(head->getContext(), "join_block", head->getParent());
        auto * if_true = llvm::BasicBlock::Create(head->getContext(), "if_true", head->getParent());
        auto * if_false = llvm::BasicBlock::Create(head->getContext(), "if_false", head->getParent());

        auto * is_predicate_true = nativeBoolCast(b, predicate_type, predicate_value);

        b.CreateCondBr(is_predicate_true, if_true, if_false);

        b.SetInsertPoint(if_true);

        size_t arguments_size_without_predicate = arguments_types.size() - 1;

        DataTypes argument_types_without_predicate;
        std::vector<llvm::Value *> argument_values_without_predicate;

        argument_types_without_predicate.resize(arguments_size_without_predicate);
        argument_values_without_predicate.resize(arguments_size_without_predicate);

        for (size_t i = 0; i < arguments_size_without_predicate; ++i)
        {
            argument_types_without_predicate[i] = arguments_types[i];
            argument_values_without_predicate[i] = argument_values[i];
        }

        nested_func->compileAdd(builder, aggregate_data_ptr, argument_types_without_predicate, argument_values_without_predicate);

        b.CreateBr(join_block);

        b.SetInsertPoint(if_false);
        b.CreateBr(join_block);

        b.SetInsertPoint(join_block);
    }

    void compileMerge(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_dst_ptr, llvm::Value * aggregate_data_src_ptr) const override
    {
        nested_func->compileMerge(builder, aggregate_data_dst_ptr, aggregate_data_src_ptr);
    }

    llvm::Value * compileGetResult(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr) const override
    {
        return nested_func->compileGetResult(builder, aggregate_data_ptr);
    }

#endif


};

}
