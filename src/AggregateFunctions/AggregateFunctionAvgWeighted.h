#pragma once

#include <type_traits>
#include <AggregateFunctions/AggregateFunctionAvg.h>

namespace DB
{
struct Settings;

template <typename T, typename U>
using MaxFieldType = std::conditional_t< //
    (sizeof(AvgFieldType<T>) > sizeof(AvgFieldType<U>)),
    AvgFieldType<T>,
    AvgFieldType<U>>;

template <typename Value, typename Weight>
class AggregateFunctionAvgWeighted final : public AggregateFunctionAvgBase<
                                               MaxFieldType<Value, Weight>,
                                               AvgFieldType<Weight>,
                                               AggregateFunctionAvgWeighted<Value, Weight>>
{
public:
    using Base = AggregateFunctionAvgBase< //
        MaxFieldType<Value, Weight>,
        AvgFieldType<Weight>,
        AggregateFunctionAvgWeighted<Value, Weight>>;
    using Base::Base;

    using Numerator = typename Base::Numerator;
    using Denominator = typename Base::Denominator;
    using Fraction = typename Base::Fraction;

    void NO_SANITIZE_UNDEFINED add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto & values = static_cast<const ColumnVectorOrDecimal<Value> &>(*columns[0]);
        const auto & weights = static_cast<const ColumnVectorOrDecimal<Weight> &>(*columns[1]);

        const Numerator value{values.getData()[row_num]};
        const Numerator weight_num{weights.getData()[row_num]};
        const Denominator weight_denom{weights.getData()[row_num]};

        this->data(place).numerator += value * weight_num;
        this->data(place).denominator += weight_denom;
    }

    String getName() const override { return "avgWeighted"; }

#if USE_EMBEDDED_COMPILER

    bool isCompilable() const override
    {
        bool can_be_compiled = Base::isCompilable();
        can_be_compiled &= canBeNativeType<Weight>();

        return can_be_compiled;
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

        auto * argument = nativeCast(b, arguments_types[0], argument_values[0], numerator_type);
        auto * weight = nativeCast(b, arguments_types[1], argument_values[1], numerator_type);

        llvm::Value * value_weight_multiplication
            = argument->getType()->isIntegerTy() ? b.CreateMul(argument, weight) : b.CreateFMul(argument, weight);
        auto * numerator_result_value = numerator_type->isIntegerTy() ? b.CreateAdd(numerator_value, value_weight_multiplication)
                                                                      : b.CreateFAdd(numerator_value, value_weight_multiplication);
        b.CreateStore(numerator_result_value, numerator_ptr);

        auto * denominator_type = toNativeType<Denominator>(b);

        static constexpr size_t denominator_offset = offsetof(Fraction, denominator);
        auto * denominator_ptr = b.CreateConstInBoundsGEP1_64(b.getInt8Ty(), aggregate_data_ptr, denominator_offset);

        auto * weight_cast_to_denominator = nativeCast(b, arguments_types[1], argument_values[1], denominator_type);

        auto * denominator_value = b.CreateLoad(denominator_type, denominator_ptr);
        auto * denominator_value_updated = denominator_type->isIntegerTy() ? b.CreateAdd(denominator_value, weight_cast_to_denominator)
                                                                           : b.CreateFAdd(denominator_value, weight_cast_to_denominator);

        b.CreateStore(denominator_value_updated, denominator_ptr);
    }

#endif
};
}
