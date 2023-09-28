#pragma once

#include <type_traits>
#include <AggregateFunctions/AggregateFunctionAvg.h>

namespace DB
{
struct Settings;

template <typename T>
using AvgWeightedFieldType = std::conditional_t<is_decimal<T>,
    std::conditional_t<std::is_same_v<T, Decimal256>, Decimal256, Decimal128>,
    std::conditional_t<DecimalOrExtendedInt<T>,
        Float64, // no way to do UInt128 * UInt128, better cast to Float64
        NearestFieldType<T>>>;

template <typename T, typename U>
using MaxFieldType = std::conditional_t<(sizeof(AvgWeightedFieldType<T>) > sizeof(AvgWeightedFieldType<U>)),
    AvgWeightedFieldType<T>, AvgWeightedFieldType<U>>;

template <typename Value, typename Weight>
class AggregateFunctionAvgWeighted final :
    public AggregateFunctionAvgBase<
        MaxFieldType<Value, Weight>, AvgWeightedFieldType<Weight>, AggregateFunctionAvgWeighted<Value, Weight>>
{
public:
    using Base = AggregateFunctionAvgBase<
        MaxFieldType<Value, Weight>, AvgWeightedFieldType<Weight>, AggregateFunctionAvgWeighted<Value, Weight>>;
    using Base::Base;

    using Numerator = typename Base::Numerator;
    using Denominator = typename Base::Denominator;
    using Fraction = typename Base::Fraction;

    void NO_SANITIZE_UNDEFINED add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto& weights = static_cast<const ColumnVectorOrDecimal<Weight> &>(*columns[1]);

        this->data(place).numerator += static_cast<Numerator>(
            static_cast<const ColumnVectorOrDecimal<Value> &>(*columns[0]).getData()[row_num]) *
            static_cast<Numerator>(weights.getData()[row_num]);

        this->data(place).denominator += static_cast<Denominator>(weights.getData()[row_num]);
    }

    String getName() const override { return "avgWeighted"; }

#if USE_EMBEDDED_COMPILER

    bool isCompilable() const override
    {
        bool can_be_compiled = Base::isCompilable();
        can_be_compiled &= canBeNativeType<Weight>();

        return can_be_compiled;
    }

    void compileAdd(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr, const ValuesWithType & arguments) const override
    {
        llvm::IRBuilder<> & b = static_cast<llvm::IRBuilder<> &>(builder);

        auto * numerator_type = toNativeType<Numerator>(b);
        auto * numerator_ptr = aggregate_data_ptr;
        auto * numerator_value = b.CreateLoad(numerator_type, numerator_ptr);

        auto numerator_data_type = toNativeDataType<Numerator>();
        auto * argument = nativeCast(b, arguments[0], numerator_data_type);
        auto * weight = nativeCast(b, arguments[1], numerator_data_type);

        llvm::Value * value_weight_multiplication = argument->getType()->isIntegerTy() ? b.CreateMul(argument, weight) : b.CreateFMul(argument, weight);
        auto * numerator_result_value = numerator_type->isIntegerTy() ? b.CreateAdd(numerator_value, value_weight_multiplication) : b.CreateFAdd(numerator_value, value_weight_multiplication);
        b.CreateStore(numerator_result_value, numerator_ptr);

        auto * denominator_type = toNativeType<Denominator>(b);

        static constexpr size_t denominator_offset = offsetof(Fraction, denominator);
        auto * denominator_ptr = b.CreateConstInBoundsGEP1_64(b.getInt8Ty(), aggregate_data_ptr, denominator_offset);

        auto * weight_cast_to_denominator = nativeCast(b, arguments[1], toNativeDataType<Denominator>());

        auto * denominator_value = b.CreateLoad(denominator_type, denominator_ptr);
        auto * denominator_value_updated = denominator_type->isIntegerTy() ? b.CreateAdd(denominator_value, weight_cast_to_denominator) : b.CreateFAdd(denominator_value, weight_cast_to_denominator);

        b.CreateStore(denominator_value_updated, denominator_ptr);
    }

#endif

};
}
