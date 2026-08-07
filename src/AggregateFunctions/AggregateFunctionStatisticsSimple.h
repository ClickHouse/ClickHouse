#pragma once

#include <cmath>
#include <memory>

#include <base/arithmeticOverflow.h>


#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/Moments.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>

#include <Core/DecimalFunctions.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Common/assert_cast.h>


/** This is simple, not numerically stable
  *  implementations of variance/covariance/correlation functions.
  *
  * It is about two times faster than stable variants.
  * Numerical errors may occur during summation.
  *
  * This implementation is selected as default,
  *  because "you don't pay for what you don't need" principle.
  *
  * For more sophisticated implementation, look at AggregateFunctionStatistics.h
  */

namespace DB
{

struct Settings;

enum class StatisticsFunctionKind : uint8_t
{
    varPop, varSamp,
    stddevPop, stddevSamp,
    skewPop, skewSamp,
    kurtPop, kurtSamp,
    covarPop, covarSamp,
    corr
};


template <typename T, size_t _level>
struct StatFuncOneArg
{
    using Type1 = T;
    using Type2 = T;
    using ResultType = std::conditional_t<std::is_same_v<T, Float32>, Float32, Float64>;
    using Data = VarMoments<ResultType, _level>;

    static constexpr UInt32 num_args = 1;
    static constexpr size_t level = _level;
};

template <typename T1, typename T2, template <typename> typename Moments>
struct StatFuncTwoArg
{
    using Type1 = T1;
    using Type2 = T2;
    using ResultType = std::conditional_t<std::is_same_v<T1, T2> && std::is_same_v<T1, Float32>, Float32, Float64>;
    using Data = Moments<ResultType>;

    static constexpr UInt32 num_args = 2;
};


template <typename StatFunc>
class AggregateFunctionVarianceSimple final
    : public IAggregateFunctionDataHelper<typename StatFunc::Data, AggregateFunctionVarianceSimple<StatFunc>>
{
public:
    using T1 = typename StatFunc::Type1;
    using T2 = typename StatFunc::Type2;
    using ColVecT1 = ColumnVectorOrDecimal<T1>;
    using ColVecT2 = ColumnVectorOrDecimal<T2>;
    using ResultType = typename StatFunc::ResultType;
    using ColVecResult = ColumnVector<ResultType>;
    using Base = IAggregateFunctionDataHelper<typename StatFunc::Data, AggregateFunctionVarianceSimple<StatFunc>>;

    explicit AggregateFunctionVarianceSimple(const DataTypes & argument_types_, StatisticsFunctionKind kind_)
        : IAggregateFunctionDataHelper<typename StatFunc::Data, AggregateFunctionVarianceSimple<StatFunc>>(argument_types_, {}, std::make_shared<DataTypeNumber<ResultType>>())
        , src_scale(0), kind(kind_)
    {
        chassert(!argument_types_.empty());
        if (isDecimal(argument_types_.front()))
        {
            src_scale = getDecimalScale(*argument_types_.front());
            if constexpr (is_decimal<T1>)
                decimal_divisor = static_cast<Float64>(DecimalUtils::scaleMultiplier<typename T1::NativeType>(src_scale));
        }
    }

    String getName() const override
    {
        return String(magic_enum::enum_name(kind));
    }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        if constexpr (StatFunc::num_args == 2)
            this->data(place).add(
                static_cast<ResultType>(static_cast<const ColVecT1 &>(*columns[0]).getData()[row_num]),
                static_cast<ResultType>(static_cast<const ColVecT2 &>(*columns[1]).getData()[row_num]));
        else
            this->data(place).add(
                convertOne(static_cast<const ColVecT1 &>(*columns[0]).getData()[row_num], decimal_divisor));
    }

    template <typename Src>
    static ALWAYS_INLINE ResultType convertOne(const Src & v, Float64 decimal_divisor)
    {
        if constexpr (std::is_same_v<Src, ResultType>)
            return v;
        else if constexpr (is_decimal<Src>)
            return static_cast<ResultType>(static_cast<Float64>(v.value) / decimal_divisor);
        else
            return static_cast<ResultType>(v);
    }

    void addBatchSinglePlace(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        Arena * arena,
        ssize_t if_argument_pos) const override
    {
        if constexpr (is_decimal<T1>)
        {
            /// A `Decimal` column holds unscaled integers, so a value is only recovered as
            /// `value / 10^scale`, and the kernel knows nothing about scale. Where that division
            /// goes depends on whether converting one element takes a call.
            ///
            /// Up to 64 bits AArch64 converts and divides with plain instructions, packed at that,
            /// so the division sits inside the accumulation loop, which stays call-free and
            /// vectorizes - 40 packed ops, no calls, 6 stack accesses for `Decimal64`.
            /// `wide::integer<128>` has no such instruction and lowers to soft-float quad calls
            /// (`__floatunditf`, `__multf3`, `__addtf3`); a call in the loop spills the lane
            /// accumulators around every element, 102 calls and 291 stack accesses against 37. So
            /// wide decimals convert into a buffer first and the accumulation reads that instead,
            /// which measured 0.950s against 1.004s. x86-64 buffers at every width: it has no
            /// packed `int64 -> double` (`vcvtqq2pd` needs AVX-512), and even `Decimal32`, which
            /// `vcvtdq2pd` could convert, measured faster through the buffer.
            ///
            /// Both shapes fold the moments at the same points and return the same bits, which is
            /// what the AArch64 one is for: dividing inline only ties the per-row path there - what
            /// `Decimal32` and `Decimal64` gain over the previous code is the inlined conversion,
            /// not the lanes - but it keeps AArch64 agreeing with x86-64.
            ///
            /// That is also why `skewPop` and `kurtPop` are left out on every target rather than
            /// just on AArch64. The per-row path gives each moment its own dependency chain, so the
            /// third and fourth are free to it - it measures the same at every level - while the
            /// lane kernel pays for them in arithmetic, and on AArch64 no lane count catches up,
            /// +7.2% at level 3 being the best. Taking the path on x86-64 alone, where it measured
            /// -7.2%, would leave the two targets disagreeing.
            if constexpr (StatFunc::num_args == 1 && StatFunc::level <= 2)
            {
                if (if_argument_pos < 0)
                {
#if defined(__x86_64__)
                    static constexpr bool convert_into_buffer = true;
#else
                    static constexpr bool convert_into_buffer = sizeof(typename T1::NativeType) > 8;
#endif
                    static constexpr size_t TILE = 1024; /// `ResultType[TILE]` stays in L1
                    const auto & vec = static_cast<const ColVecT1 &>(*columns[0]).getData();
                    auto & data = this->data(place);

                    [[maybe_unused]] ResultType buf[convert_into_buffer ? TILE : 1];
                    for (size_t off = row_begin; off < row_end; off += TILE)
                    {
                        const size_t tile = std::min(TILE, row_end - off);
                        if constexpr (convert_into_buffer)
                        {
                            for (size_t k = 0; k < tile; ++k)
                                buf[k] = convertOne(vec[off + k], decimal_divisor);
                            data.addMany(buf, /*row_begin=*/0, tile);
                        }
                        else
                            data.addManyDivided(vec.data() + off, decimal_divisor, /*row_begin=*/0, tile);
                    }
                    return;
                }
            }

            /// Conditional aggregation, the two-argument kinds and the higher moments stay on the
            /// generic per-row path.
            Base::addBatchSinglePlace(row_begin, row_end, place, columns, arena, if_argument_pos);
        }
        else
        {
            auto & data = this->data(place);
            if (if_argument_pos >= 0)
            {
                const auto * flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData().data();
                if constexpr (StatFunc::num_args == 2)
                    data.template addManyConditional<T1, T2, false>(
                        static_cast<const ColVecT1 &>(*columns[0]).getData().data(),
                        static_cast<const ColVecT2 &>(*columns[1]).getData().data(),
                        flags, row_begin, row_end);
                else
                    data.template addManyConditional<T1, false>(
                        static_cast<const ColVecT1 &>(*columns[0]).getData().data(), flags, row_begin, row_end);
            }
            else
            {
                if constexpr (StatFunc::num_args == 2)
                    data.addMany(
                        static_cast<const ColVecT1 &>(*columns[0]).getData().data(),
                        static_cast<const ColVecT2 &>(*columns[1]).getData().data(),
                        row_begin, row_end);
                else
                    data.addMany(static_cast<const ColVecT1 &>(*columns[0]).getData().data(), row_begin, row_end);
            }
        }
    }

    void addBatchSinglePlaceNotNull(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        const UInt8 * null_map,
        Arena * arena,
        ssize_t if_argument_pos) const override
    {
        if constexpr (is_decimal<T1>)
        {
            Base::addBatchSinglePlaceNotNull(row_begin, row_end, place, columns, null_map, arena, if_argument_pos);
        }
        else
        {
            auto & data = this->data(place);
            if (if_argument_pos >= 0)
            {
                /// Merging the two sets of flags into a temporary buffer vectorizes better
                /// than fusing both flags into the accumulation loop.
                const auto * if_flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData().data();
                /// Default-init: the loop below fills [row_begin, row_end) and nothing reads the rest.
                std::unique_ptr<UInt8[]> final_flags(new UInt8[row_end]);
                for (size_t i = row_begin; i < row_end; ++i)
                    final_flags[i] = (!null_map[i]) & !!if_flags[i];

                if constexpr (StatFunc::num_args == 2)
                    data.template addManyConditional<T1, T2, false>(
                        static_cast<const ColVecT1 &>(*columns[0]).getData().data(),
                        static_cast<const ColVecT2 &>(*columns[1]).getData().data(),
                        final_flags.get(), row_begin, row_end);
                else
                    data.template addManyConditional<T1, false>(
                        static_cast<const ColVecT1 &>(*columns[0]).getData().data(), final_flags.get(), row_begin, row_end);
            }
            else
            {
                if constexpr (StatFunc::num_args == 2)
                    data.template addManyConditional<T1, T2, true>(
                        static_cast<const ColVecT1 &>(*columns[0]).getData().data(),
                        static_cast<const ColVecT2 &>(*columns[1]).getData().data(),
                        null_map, row_begin, row_end);
                else
                    data.template addManyConditional<T1, true>(
                        static_cast<const ColVecT1 &>(*columns[0]).getData().data(), null_map, row_begin, row_end);
            }
        }
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        this->data(place).read(buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        const auto & data = this->data(place);
        auto & dst = static_cast<ColVecResult &>(to).getData();

        switch (kind)
        {
            case StatisticsFunctionKind::varPop:
            {
                dst.push_back(data.getPopulation());
                break;
            }
            case StatisticsFunctionKind::varSamp:
            {
                dst.push_back(data.getSample());
                break;
            }
            case StatisticsFunctionKind::stddevPop:
            {
                dst.push_back(std::sqrt(data.getPopulation()));
                break;
            }
            case StatisticsFunctionKind::stddevSamp:
            {
                dst.push_back(std::sqrt(data.getSample()));
                break;
            }
            case StatisticsFunctionKind::skewPop:
            {
                ResultType var_value = data.getPopulation();

                if (var_value > 0)
                    dst.push_back(static_cast<ResultType>(static_cast<Float64>(data.getMoment3()) / std::pow(static_cast<Float64>(var_value), 1.5)));
                else
                    dst.push_back(std::numeric_limits<ResultType>::quiet_NaN());

                break;
            }
            case StatisticsFunctionKind::skewSamp:
            {
                ResultType var_value = data.getSample();

                if (var_value > 0)
                    dst.push_back(static_cast<ResultType>(static_cast<Float64>(data.getMoment3()) / std::pow(static_cast<Float64>(var_value), 1.5)));
                else
                    dst.push_back(std::numeric_limits<ResultType>::quiet_NaN());

                break;
            }
            case StatisticsFunctionKind::kurtPop:
            {
                ResultType var_value = data.getPopulation();

                if (var_value > 0)
                    dst.push_back(static_cast<ResultType>(static_cast<Float64>(data.getMoment4()) / std::pow(static_cast<Float64>(var_value), 2.0)));
                else
                    dst.push_back(std::numeric_limits<ResultType>::quiet_NaN());

                break;
            }
            case StatisticsFunctionKind::kurtSamp:
            {
                ResultType var_value = data.getSample();

                if (var_value > 0)
                    dst.push_back(static_cast<ResultType>(static_cast<Float64>(data.getMoment4()) / std::pow(static_cast<Float64>(var_value), 2.0)));
                else
                    dst.push_back(std::numeric_limits<ResultType>::quiet_NaN());

                break;
            }
            case StatisticsFunctionKind::covarPop:
            {
                dst.push_back(data.getPopulation());
                break;
            }
            case StatisticsFunctionKind::covarSamp:
            {
                dst.push_back(data.getSample());
                break;
            }
            case StatisticsFunctionKind::corr:
            {
                dst.push_back(data.get());
                break;
            }
        }
    }

private:
    UInt32 src_scale;
    Float64 decimal_divisor = 1; /// 10^src_scale as Float64, for the inline Decimal -> Float64 convert
    StatisticsFunctionKind kind;
};


struct Settings;

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

template <template <typename> typename FunctionTemplate, StatisticsFunctionKind kind>
AggregateFunctionPtr createAggregateFunctionStatisticsUnary(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    AggregateFunctionPtr res;
    const DataTypePtr & data_type = argument_types[0];
    if (isDecimal(data_type))
        res.reset(createWithDecimalType<FunctionTemplate>(*data_type, argument_types, kind));
    else
        res.reset(createWithNumericType<FunctionTemplate>(*data_type, argument_types, kind));

    if (!res)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument for aggregate function {}",
                        argument_types[0]->getName(), name);
    return res;
}

template <template <typename, typename> typename FunctionTemplate, StatisticsFunctionKind kind>
AggregateFunctionPtr createAggregateFunctionStatisticsBinary(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    assertBinary(name, argument_types);

    AggregateFunctionPtr res(createWithTwoBasicNumericTypes<FunctionTemplate>(*argument_types[0], *argument_types[1], argument_types, kind));
    if (!res)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal types {} and {} of arguments for aggregate function {}",
            argument_types[0]->getName(), argument_types[1]->getName(), name);

    return res;
}

}
