#pragma once

#include <algorithm>
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

    /// Convert one source element to `ResultType`. For `Decimal` this is done inline -
    /// `value / 10^scale` - rather than via the out-of-line `convertFromDecimal`, so the conversion
    /// can itself auto-vectorize (`scvtf`/`fdiv` on AArch64, `vcvtdq2pd`/`vdivpd` for `Decimal32` on
    /// x86); the value is identical to `convertFromDecimal` (same `convertToImpl` math).
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
            /// A `Decimal` column holds unscaled integers. The scale lives in the data type, not in
            /// the column, so an element only becomes a number as `value / 10^scale`. `addMany` is a
            /// member of `VarMoments` and knows nothing about scale - and handing it the column would
            /// not even fail to compile, because `Decimal` converts implicitly to its native integer
            /// (`Decimal::operator T`), so it would quietly accumulate the unscaled integers. Hence
            /// the conversion has to happen before the kernel sees the values.
            ///
            /// Converting the whole range into one buffer would push it out to memory and read it
            /// back, so the values are converted a tile at a time and each tile is accumulated while
            /// it is still in L1. Keeping the two loops separate is also what lets the accumulation
            /// vectorize on `x86-64-v3`, which has no packed `int64 -> double` (`vcvtqq2pd` arrived
            /// with AVX-512): in a fused convert-and-accumulate loop the scalar converts would drag
            /// the accumulation back down with them. Alone in its own loop, the conversion vectorizes
            /// on its own where the target allows it - `vcvtdq2pd`/`vdivpd` for `Decimal32` on x86,
            /// `scvtf`/`fdiv` on AArch64.
            if constexpr (StatFunc::num_args == 1)
            {
                if (if_argument_pos < 0)
                {
                    static constexpr size_t TILE = 1024; /// `ResultType[TILE]` stays in L1
                    const auto & vec = static_cast<const ColVecT1 &>(*columns[0]).getData();
                    auto & data = this->data(place);
                    ResultType buf[TILE];
                    for (size_t off = row_begin; off < row_end; off += TILE)
                    {
                        const size_t tile = std::min(TILE, row_end - off);
                        for (size_t k = 0; k < tile; ++k)
                            buf[k] = convertOne(vec[off + k], decimal_divisor);
                        data.addMany(buf, 0, tile);
                    }
                    return;
                }
            }

            /// Conditional and two-argument aggregation over `Decimal` stay on the generic per-row path.
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
