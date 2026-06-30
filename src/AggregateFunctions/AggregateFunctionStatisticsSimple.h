#pragma once

#include <algorithm>
#include <cmath>

#include <base/arithmeticOverflow.h>


#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/Moments.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>

#include <Core/DecimalFunctions.h>
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

    /// Accumulate `len` contiguous source elements (converted to `ResultType` inline) into `data`
    /// using `W` independent struct-of-arrays lane accumulators (`s1[W]`, `s2[W]`, ...), then fold
    /// the lanes into the moment state. The lanes are local so the `W`-wide inner loop holds them in
    /// vector registers and auto-vectorizes (`vaddpd`/`vmulpd` over `ymm` for `Float64`); the fixed
    /// lane layout keeps the summation order deterministic across builds and platforms.
    template <typename Src>
    static ALWAYS_INLINE void addManyTyped(const Src * __restrict src, size_t len, Float64 decimal_divisor, typename StatFunc::Data & data)
    {
        static constexpr size_t W = 4;
        static constexpr size_t level = StatFunc::level;

        ResultType s1[W]{};
        ResultType s2[W]{};
        [[maybe_unused]] ResultType s3[W]{};
        [[maybe_unused]] ResultType s4[W]{};

        const size_t vectorized = len / W * W;
        for (size_t i = 0; i < vectorized; i += W)
            for (size_t s = 0; s < W; ++s)
            {
                const ResultType x = convertOne(src[i + s], decimal_divisor);
                s1[s] += x;
                s2[s] += x * x;
                if constexpr (level >= 3) s3[s] += x * x * x;
                if constexpr (level >= 4) s4[s] += x * x * x * x;
            }

        /// Fold the lanes with an explicit fixed-order horizontal sum. A reduction *loop* here
        /// would force the lane arrays to stay addressable in memory and degrade the main loop to
        /// 128-bit; the unrolled form lets the lanes live in `ymm` registers. `W == 4` assumed.
        static_assert(W == 4);
        data.m[1] += s1[0] + s1[1] + s1[2] + s1[3];
        data.m[2] += s2[0] + s2[1] + s2[2] + s2[3];
        if constexpr (level >= 3) data.m[3] += s3[0] + s3[1] + s3[2] + s3[3];
        if constexpr (level >= 4) data.m[4] += s4[0] + s4[1] + s4[2] + s4[3];
        data.m[0] += static_cast<ResultType>(vectorized);

        for (size_t i = vectorized; i < len; ++i)
            data.add(convertOne(src[i], decimal_divisor));
    }

    /// Vectorizable fast path for the common single-argument, unconditional case.
    ///
    /// The per-row `add` reduces into a single moment state, so the loop-carried dependency on
    /// `m[1]`/`m[2]` serializes it and blocks auto-vectorization. `addManyContiguous` instead
    /// accumulates into `W` independent struct-of-arrays lanes, which vectorizes for `Float64`.
    ///
    /// For types that need a per-element conversion (integers, `Decimal`) the strategy is
    /// architecture-dependent. x86-64-v3 has no packed `int64 -> double` (`vcvtqq2pd` is AVX-512
    /// only), so converting 64-bit values cannot vectorize; there we convert a small cache-resident
    /// tile first and then accumulate it with a separate vectorized pass, so the scalar conversion
    /// does not serialize the reduction. On targets with a packed integer->double convert (e.g.
    /// AArch64 `scvtf`) the conversion vectorizes inline, so a single direct pass is faster than
    /// staging through a buffer.
    ///
    /// The lane layout changes summation order relative to a strict sequential sum (last-bit float
    /// differences). Because lanes are folded per `addBatchSinglePlace` call, the result also
    /// depends on the block size - the same way parallel aggregation already makes float
    /// `sum`/`avg`/variance non-bit-reproducible across `max_threads`; the "simple" variance is
    /// documented as numerically unstable, so this is within contract. For a fixed chunking the
    /// reduction order is fully determined by the source (lane layout + explicit horizontal sum,
    /// independent of whether the compiler vectorizes), so it is reproducible across builds and
    /// platforms. Conditional aggregation (`if_argument_pos >= 0`) and the two-argument kinds fall
    /// back to the scalar base loop.
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
            if (if_argument_pos < 0 && row_end > row_begin)
            {
                static constexpr size_t W = 4;

                const auto & vec = static_cast<const ColVecT1 &>(*columns[0]).getData();
                const T1 * __restrict ptr = vec.data() + row_begin;
                const size_t total = row_end - row_begin;
                auto & data = this->data(place);

                if constexpr (std::is_same_v<T1, ResultType>)
                {
                    /// No conversion needed (`Float32`/`Float64`): accumulate directly off the column.
                    addManyTyped(ptr, total, decimal_divisor, data);
                }
                else
                {
#if defined(__x86_64__)
                    /// Conversion of 64-bit values does not vectorize here; stage a cache-resident
                    /// tile of converted values, then accumulate it with a vectorized pass.
                    static constexpr size_t TILE = 1024; /// multiple of W; ResultType[TILE] stays in L1
                    ResultType buf[TILE];
                    for (size_t off = 0; off + W <= total; off += TILE)
                    {
                        const size_t tile = std::min(TILE, (total - off) / W * W);
                        for (size_t k = 0; k < tile; ++k)
                            buf[k] = convertOne(ptr[off + k], decimal_divisor);
                        addManyTyped(buf, tile, decimal_divisor, data);
                    }

                    /// Overall tail (fewer than `W` elements) that no full tile covered.
                    for (size_t i = total / W * W; i < total; ++i)
                        data.add(convertOne(ptr[i], decimal_divisor));
#else
                    /// Conversion vectorizes inline (e.g. AArch64 `scvtf`): one direct pass, no buffer.
                    addManyTyped(ptr, total, decimal_divisor, data);
#endif
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
