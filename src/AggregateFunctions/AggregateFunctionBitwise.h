#pragma once

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnVector.h>
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


template <typename T>
struct AggregateFunctionGroupBitOrData
{
    T value = 0;
    static const char * name() { return "groupBitOr"; }
    void update(T x) { value |= x; }

#if USE_EMBEDDED_COMPILER

    static void compileCreate(llvm::IRBuilderBase & builder, llvm::Value * value_ptr)
    {
        auto type = toNativeType<T>(builder);
        builder.CreateStore(llvm::Constant::getNullValue(type), value_ptr);
    }

    static llvm::Value* compileUpdate(llvm::IRBuilderBase & builder, llvm::Value * lhs, llvm::Value * rhs)
    {
        return builder.CreateOr(lhs, rhs);
    }

#endif
};

template <typename T>
struct AggregateFunctionGroupBitAndData
{
    T value = -1; /// Two's complement arithmetic, sign extension.
    static const char * name() { return "groupBitAnd"; }
    void update(T x) { value &= x; }

#if USE_EMBEDDED_COMPILER

    static void compileCreate(llvm::IRBuilderBase & builder, llvm::Value * value_ptr)
    {
        auto type = toNativeType<T>(builder);
        builder.CreateStore(llvm::ConstantInt::get(type, -1), value_ptr);
    }

    static llvm::Value* compileUpdate(llvm::IRBuilderBase & builder, llvm::Value * lhs, llvm::Value * rhs)
    {
        return builder.CreateAnd(lhs, rhs);
    }

#endif
};

template <typename T>
struct AggregateFunctionGroupBitXorData
{
    T value = 0;
    static const char * name() { return "groupBitXor"; }
    void update(T x) { value ^= x; }

#if USE_EMBEDDED_COMPILER

    static void compileCreate(llvm::IRBuilderBase & builder, llvm::Value * value_ptr)
    {
        auto type = toNativeType<T>(builder);
        builder.CreateStore(llvm::Constant::getNullValue(type), value_ptr);
    }

    static llvm::Value* compileUpdate(llvm::IRBuilderBase & builder, llvm::Value * lhs, llvm::Value * rhs)
    {
        return builder.CreateXor(lhs, rhs);
    }

#endif
};


/// Counts bitwise operation on numbers.
template <typename T, typename Data>
class AggregateFunctionBitwise final : public IAggregateFunctionDataHelper<Data, AggregateFunctionBitwise<T, Data>>
{
public:
    explicit AggregateFunctionBitwise(const DataTypePtr & type)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionBitwise<T, Data>>({type}, {}) {}

    String getName() const override { return Data::name(); }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeNumber<T>>();
    }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        this->data(place).update(assert_cast<const ColumnVector<T> &>(*columns[0]).getData()[row_num]);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).update(this->data(rhs).value);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        writeBinary(this->data(place).value, buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        readBinary(this->data(place).value, buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        assert_cast<ColumnVector<T> &>(to).getData().push_back(this->data(place).value);
    }

#if USE_EMBEDDED_COMPILER

    bool isCompilable() const override
    {
        auto return_type = getReturnType();
        return canBeNativeType(*return_type);
    }

    void compileCreate(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr) const override
    {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        auto * return_type = toNativeType(b, getReturnType());
        auto * value_ptr = b.CreatePointerCast(aggregate_data_ptr, return_type->getPointerTo());
        Data::compileCreate(builder, value_ptr);
    }

    void compileAdd(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr, const DataTypes &, const std::vector<llvm::Value *> & argument_values) const override
    {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        auto * return_type = toNativeType(b, getReturnType());

        auto * value_ptr = b.CreatePointerCast(aggregate_data_ptr, return_type->getPointerTo());
        auto * value = b.CreateLoad(return_type, value_ptr);

        const auto & argument_value = argument_values[0];
        auto * result_value = Data::compileUpdate(builder, value, argument_value);

        b.CreateStore(result_value, value_ptr);
    }

    void compileMerge(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_dst_ptr, llvm::Value * aggregate_data_src_ptr) const override
    {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        auto * return_type = toNativeType(b, getReturnType());

        auto * value_dst_ptr = b.CreatePointerCast(aggregate_data_dst_ptr, return_type->getPointerTo());
        auto * value_dst = b.CreateLoad(return_type, value_dst_ptr);

        auto * value_src_ptr = b.CreatePointerCast(aggregate_data_src_ptr, return_type->getPointerTo());
        auto * value_src = b.CreateLoad(return_type, value_src_ptr);

        auto * result_value = Data::compileUpdate(builder, value_dst, value_src);

        b.CreateStore(result_value, value_dst_ptr);
    }

    llvm::Value * compileGetResult(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr) const override
    {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        auto * return_type = toNativeType(b, getReturnType());
        auto * value_ptr = b.CreatePointerCast(aggregate_data_ptr, return_type->getPointerTo());

        return b.CreateLoad(return_type, value_ptr);
    }

#endif

};


}
