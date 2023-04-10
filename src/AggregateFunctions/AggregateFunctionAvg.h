#pragma once

#include <type_traits>
#include "AggregateFunctions/AggregateFunctionSum.h"
#include "AggregateFunctions/IAggregateFunction.h"
#include "Columns/ColumnDecimal.h"
#include "Columns/ColumnsCommon.h"
#include "Core/Field.h"
#include "base/Decimal.h"
#include "base/types.h"
#include "config.h"

#if USE_EMBEDDED_COMPILER
#    include <DataTypes/Native.h>
#    include <llvm/IR/IRBuilder.h>
#endif

namespace DB
{
struct Settings;

template <class Numerator>
struct AvgState
{
    Numerator numerator{0};
    UInt64 denominator{0};
};

template <class T>
using AvgFieldType = std::conditional_t< //
    is_decimal<T>,
    std::conditional_t<std::is_same_v<T, Decimal256>, Decimal256, Decimal128>,
    NearestFieldType<T>>;

template <class T>
class AggregateFunctionAvg : public IAggregateFunctionDataHelper<AvgState<AvgFieldType<T>>, AggregateFunctionAvg<T>>
{
public:
    using Fraction = AvgState<AvgFieldType<T>>;
    using Base = IAggregateFunctionDataHelper<Fraction, AggregateFunctionAvg<T>>;
    using Numerator = AvgFieldType<T>;
    using ColVecType = ColumnVectorOrDecimal<T>;

    explicit AggregateFunctionAvg(const DataTypes & argument_types_, UInt32 numerator_scale_ = 0)
        : Base(argument_types_, {}, createResultType()), numerator_scale(numerator_scale_)
    {
    }

    AggregateFunctionAvg(const DataTypes & argument_types_, const DataTypePtr & result_type_, UInt32 numerator_scale_ = 0)
        : Base(argument_types_, {}, result_type_), numerator_scale(numerator_scale_)
    {
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const final
    {
        this->data(place).numerator += static_cast<const ColVecType &>(*columns[0]).getData()[row_num];
        ++this->data(place).denominator;
    }

    void addManyDefaults(AggregateDataPtr __restrict place, const IColumn **, size_t length, Arena *) const override
    {
        this->data(place).denominator += length;
    }

    void addBatchSinglePlace(
        size_t row_begin, size_t row_end, AggregateDataPtr __restrict place, const IColumn ** columns, Arena *, ssize_t if_argument_pos)
        const final
    {
        AggregateFunctionSumData<Numerator> sum_data;
        const auto & column = assert_cast<const ColVecType &>(*columns[0]);
        if (if_argument_pos >= 0)
        {
            const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            sum_data.addManyConditional(column.getData().data(), flags.data(), row_begin, row_end);
            this->data(place).denominator += countBytesInFilter(flags.data(), row_begin, row_end);
        }
        else
        {
            sum_data.addMany(column.getData().data(), row_begin, row_end);
            this->data(place).denominator += (row_end - row_begin);
        }
        this->data(place).numerator += sum_data.sum;
    }

    void addBatchSinglePlaceNotNull(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        const UInt8 * null_map,
        Arena *,
        ssize_t if_argument_pos) const final
    {
        AggregateFunctionSumData<Numerator> sum_data;
        const auto & column = assert_cast<const ColVecType &>(*columns[0]);
        if (if_argument_pos >= 0)
        {
            /// Merge the 2 sets of flags (null and if) into a single one. This allows us to use parallelizable sums when available
            const auto * if_flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData().data();
            auto final_flags = std::make_unique<UInt8[]>(row_end);
            size_t used_value = 0;
            for (size_t i = row_begin; i < row_end; ++i)
            {
                UInt8 kept = (!null_map[i]) & !!if_flags[i];
                final_flags[i] = kept;
                used_value += kept;
            }

            sum_data.addManyConditional(column.getData().data(), final_flags.get(), row_begin, row_end);
            this->data(place).denominator += used_value;
        }
        else
        {
            sum_data.addManyNotNull(column.getData().data(), null_map, row_begin, row_end);
            this->data(place).denominator += (row_end - row_begin) - countBytesInFilter(null_map, row_begin, row_end);
        }

        this->data(place).numerator += sum_data.sum;
    }

    String getName() const override { return "avg"; }

    DataTypePtr createResultType() const { return std::make_shared<DataTypeNumber<Float64>>(); }

    constexpr bool allocatesMemoryInArena() const final { return false; }

    void NO_SANITIZE_UNDEFINED merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).numerator += this->data(rhs).numerator;
        this->data(place).denominator += this->data(rhs).denominator;
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> version) const final
    {
        if (!version)
        {
            writeBinary(this->data(place).numerator, buf);
            writeVarUInt(0, buf);
        }
        else if (*version == 0)
            writeBinary(this->data(place).numerator, buf);
        else
            throwError("Too old server version to read function state");
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> version, Arena *) const final
    {
        if (!version)
        {
            readBinary(this->data(place).numerator, buf);
            UInt64 tmp;
            readVarUInt(tmp, buf);
        }
        else if (*version == 0)
            readBinary(this->data(place).numerator, buf);
        else
            throwError("Too old server version to read function state");
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        assert_cast<ColumnVector<Float64> &>(to).getData().push_back(divide(place));
    }

#if USE_EMBEDDED_COMPILER
    bool isCompilable() const override
    {
        bool can_be_compiled = true;

        for (const auto & argument : this->argument_types)
            can_be_compiled &= canBeNativeType(*argument);

        auto return_type = this->getResultType();
        can_be_compiled &= canBeNativeType(*return_type);

        return can_be_compiled;
    }

    void compileCreate(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr) const override
    {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);
        b.CreateMemSet(
            aggregate_data_ptr, llvm::ConstantInt::get(b.getInt8Ty(), 0), sizeof(Fraction), llvm::assumeAligned(this->alignOfData()));
    }

    void
    compileMerge(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_dst_ptr, llvm::Value * aggregate_data_src_ptr) const override
    {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        auto * numerator_type = toNativeType<Numerator>(b);

        auto * numerator_dst_ptr = aggregate_data_dst_ptr;
        auto * numerator_dst_value = b.CreateLoad(numerator_type, numerator_dst_ptr);

        auto * numerator_src_ptr = aggregate_data_src_ptr;
        auto * numerator_src_value = b.CreateLoad(numerator_type, numerator_src_ptr);

        auto * numerator_result_value = numerator_type->isIntegerTy() ? b.CreateAdd(numerator_dst_value, numerator_src_value)
                                                                      : b.CreateFAdd(numerator_dst_value, numerator_src_value);
        b.CreateStore(numerator_result_value, numerator_dst_ptr);

        auto * denominator_type = toNativeType<UInt64>(b);
        static constexpr size_t denominator_offset = offsetof(Fraction, denominator);
        auto * denominator_dst_ptr = b.CreateConstInBoundsGEP1_64(b.getInt8Ty(), aggregate_data_dst_ptr, denominator_offset);
        auto * denominator_src_ptr = b.CreateConstInBoundsGEP1_64(b.getInt8Ty(), aggregate_data_src_ptr, denominator_offset);

        auto * denominator_dst_value = b.CreateLoad(denominator_type, denominator_dst_ptr);
        auto * denominator_src_value = b.CreateLoad(denominator_type, denominator_src_ptr);

        auto * denominator_result_value = denominator_type->isIntegerTy() ? b.CreateAdd(denominator_src_value, denominator_dst_value)
                                                                          : b.CreateFAdd(denominator_src_value, denominator_dst_value);
        b.CreateStore(denominator_result_value, denominator_dst_ptr);
    }

    llvm::Value * compileGetResult(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr) const override
    {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        auto * numerator_type = toNativeType<Numerator>(b);
        auto * numerator_ptr = aggregate_data_ptr;
        auto * numerator_value = b.CreateLoad(numerator_type, numerator_ptr);

        auto * denominator_type = toNativeType<UInt64>(b);
        static constexpr size_t denominator_offset = offsetof(Fraction, denominator);
        auto * denominator_ptr = b.CreateConstGEP1_32(b.getInt8Ty(), aggregate_data_ptr, denominator_offset);
        auto * denominator_value = b.CreateLoad(denominator_type, denominator_ptr);

        auto * double_numerator = nativeCast<Numerator>(b, numerator_value, b.getDoubleTy());
        auto * double_denominator = nativeCast<UInt64>(b, denominator_value, b.getDoubleTy());

        return b.CreateFDiv(double_numerator, double_denominator);
    }

    void compileAdd(
        llvm::IRBuilderBase & builder,
        llvm::Value * aggregate_data_ptr,
        const DataTypes & arguments_types,
        const std::vector<llvm::Value *> & argument_values) const override
    {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        auto * numerator_type = toNativeType<Numerator>(b);

        auto * numerator_ptr = aggregate_data_ptr;
        auto * numerator_value = b.CreateLoad(numerator_type, numerator_ptr);
        auto * value_cast_to_numerator = nativeCast(b, arguments_types[0], argument_values[0], numerator_type);
        auto * numerator_result_value = numerator_type->isIntegerTy() ? b.CreateAdd(numerator_value, value_cast_to_numerator)
                                                                      : b.CreateFAdd(numerator_value, value_cast_to_numerator);
        b.CreateStore(numerator_result_value, numerator_ptr);

        auto * denominator_type = toNativeType<UInt64>(b);
        static constexpr size_t denominator_offset = offsetof(Fraction, denominator);
        auto * denominator_ptr = b.CreateConstGEP1_32(b.getInt8Ty(), aggregate_data_ptr, denominator_offset);
        auto * denominator_value_updated
            = b.CreateAdd(b.CreateLoad(denominator_type, denominator_ptr), llvm::ConstantInt::get(denominator_type, 1));
        b.CreateStore(denominator_value_updated, denominator_ptr);
    }

#endif

private:
    UInt32 numerator_scale;

    Float64 NO_SANITIZE_UNDEFINED divide(AggregateDataPtr __restrict place) const
    {
        const auto & numerator = this->data(place).numerator;
        const UInt64 denominator = this->data(place).denominator;

        if constexpr (is_decimal<Numerator>)
            return DecimalUtils::convertTo<Float64>(numerator, numerator_scale) / denominator;
        else
            return static_cast<Float64>(numerator) / denominator;
    }
};
}
