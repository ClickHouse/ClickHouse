#pragma once

#include <cmath>

#include <base/arithmeticOverflow.h>


#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/Moments.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Columns/ColumnVector.h>


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

    explicit AggregateFunctionVarianceSimple(const DataTypes & argument_types_, StatisticsFunctionKind kind_)
        : IAggregateFunctionDataHelper<typename StatFunc::Data, AggregateFunctionVarianceSimple<StatFunc>>(argument_types_, {}, std::make_shared<DataTypeNumber<ResultType>>())
        , src_scale(0), kind(kind_)
    {
        chassert(!argument_types_.empty());
        if (isDecimal(argument_types_.front()))
            src_scale = getDecimalScale(*argument_types_.front());
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
        {
            if constexpr (is_decimal<T1>)
            {
                this->data(place).add(
                    convertFromDecimal<DataTypeDecimal<T1>, DataTypeFloat64>(
                        static_cast<const ColVecT1 &>(*columns[0]).getData()[row_num], src_scale));
            }
            else
                this->data(place).add(
                    static_cast<ResultType>(static_cast<const ColVecT1 &>(*columns[0]).getData()[row_num]));
        }
    }

    /// Vectorizable fast path for the common single-argument, unconditional case.
    /// The per-row `add` reduces into a single moment state, which serializes the loop and
    /// blocks auto-vectorization. Here we accumulate into `unroll` independent partial states
    /// (no cross-iteration dependency) directly off the typed column pointer, then merge them.
    /// This changes summation order relative to a strict sequential sum (last-bit float
    /// differences), but the result is identical across builds and platforms - consistent with
    /// how `merge` already tree-reduces partial states across blocks. Conditional aggregation
    /// (`if_argument_pos >= 0`) and the two-argument kinds fall back to the scalar base loop.
    void addBatchSinglePlace(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** __restrict columns,
        Arena * arena,
        ssize_t if_argument_pos = -1) const override
    {
        if constexpr (StatFunc::num_args == 1)
        {
            if (if_argument_pos < 0)
            {
                const auto & vec = static_cast<const ColVecT1 &>(*columns[0]).getData();
                const T1 * __restrict ptr = vec.data();

                static constexpr size_t unroll = 4;
                typename StatFunc::Data partial[unroll];

                size_t i = row_begin;
                const size_t unrolled_end = row_begin + (row_end - row_begin) / unroll * unroll;

                for (; i < unrolled_end; i += unroll)
                    for (size_t s = 0; s < unroll; ++s)
                    {
                        if constexpr (is_decimal<T1>)
                            partial[s].add(convertFromDecimal<DataTypeDecimal<T1>, DataTypeFloat64>(ptr[i + s], src_scale));
                        else
                            partial[s].add(static_cast<ResultType>(ptr[i + s]));
                    }

                auto & data = this->data(place);
                for (const auto & p : partial)
                    data.merge(p);

                for (; i < row_end; ++i)
                {
                    if constexpr (is_decimal<T1>)
                        data.add(convertFromDecimal<DataTypeDecimal<T1>, DataTypeFloat64>(ptr[i], src_scale));
                    else
                        data.add(static_cast<ResultType>(ptr[i]));
                }
                return;
            }
        }

        IAggregateFunctionDataHelper<typename StatFunc::Data, AggregateFunctionVarianceSimple<StatFunc>>::addBatchSinglePlace(
            row_begin, row_end, place, columns, arena, if_argument_pos);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
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
