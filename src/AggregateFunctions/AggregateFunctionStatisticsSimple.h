#pragma once

#include <cmath>

#include <common/arithmeticOverflow.h>

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <AggregateFunctions/IAggregateFunction.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnDecimal.h>


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

namespace ErrorCodes
{
    extern const int DECIMAL_OVERFLOW;
}


/**
    Calculating univariate central moments
    Levels:
        level 2 (pop & samp): var, stddev
        level 3: skewness
        level 4: kurtosis
    References:
        https://en.wikipedia.org/wiki/Moment_(mathematics)
        https://en.wikipedia.org/wiki/Skewness
        https://en.wikipedia.org/wiki/Kurtosis
*/
template <typename T, size_t _level>
struct VarMoments
{
    T m[_level + 1]{};

    void add(T x)
    {
        ++m[0];
        m[1] += x;
        m[2] += x * x;
        if constexpr (_level >= 3) m[3] += x * x * x;
        if constexpr (_level >= 4) m[4] += x * x * x * x;
    }

    void merge(const VarMoments & rhs)
    {
        m[0] += rhs.m[0];
        m[1] += rhs.m[1];
        m[2] += rhs.m[2];
        if constexpr (_level >= 3) m[3] += rhs.m[3];
        if constexpr (_level >= 4) m[4] += rhs.m[4];
    }

    void write(WriteBuffer & buf) const
    {
        writePODBinary(*this, buf);
    }

    void read(ReadBuffer & buf)
    {
        readPODBinary(*this, buf);
    }

    T getPopulation() const
    {
        if (m[0] == 0)
            return std::numeric_limits<T>::quiet_NaN();

        /// Due to numerical errors, the result can be slightly less than zero,
        /// but it should be impossible. Trim to zero.

        return std::max(T{}, (m[2] - m[1] * m[1] / m[0]) / m[0]);
    }

    T getSample() const
    {
        if (m[0] <= 1)
            return std::numeric_limits<T>::quiet_NaN();
        return std::max(T{}, (m[2] - m[1] * m[1] / m[0]) / (m[0] - 1));
    }

    T getMoment3() const
    {
        if (m[0] == 0)
            return std::numeric_limits<T>::quiet_NaN();
        // to avoid accuracy problem
        if (m[0] == 1)
            return 0;
        return (m[3]
            - (3 * m[2]
                - 2 * m[1] * m[1] / m[0]
            ) * m[1] / m[0]
        ) / m[0];
    }

    T getMoment4() const
    {
        if (m[0] == 0)
            return std::numeric_limits<T>::quiet_NaN();
        // to avoid accuracy problem
        if (m[0] == 1)
            return 0;
        return (m[4]
            - (4 * m[3]
                - (6 * m[2]
                    - 3 * m[1] * m[1] / m[0]
                ) * m[1] / m[0]
            ) * m[1] / m[0]
        ) / m[0];
    }
};

template <typename T, size_t _level>
struct VarMomentsDecimal
{
    using NativeType = typename T::NativeType;

    UInt64 m0{};
    NativeType m[_level]{};

    NativeType & getM(size_t i)
    {
        return m[i - 1];
    }

    const NativeType & getM(size_t i) const
    {
        return m[i - 1];
    }

    void add(NativeType x)
    {
        ++m0;
        getM(1) += x;

        NativeType tmp;
        if (common::mulOverflow(x, x, tmp) || common::addOverflow(getM(2), tmp, getM(2)))
            throw Exception("Decimal math overflow", ErrorCodes::DECIMAL_OVERFLOW);
        if constexpr (_level >= 3)
            if (common::mulOverflow(tmp, x, tmp) || common::addOverflow(getM(3), tmp, getM(3)))
                throw Exception("Decimal math overflow", ErrorCodes::DECIMAL_OVERFLOW);
        if constexpr (_level >= 4)
            if (common::mulOverflow(tmp, x, tmp) || common::addOverflow(getM(4), tmp, getM(4)))
                throw Exception("Decimal math overflow", ErrorCodes::DECIMAL_OVERFLOW);
    }

    void merge(const VarMomentsDecimal & rhs)
    {
        m0 += rhs.m0;
        getM(1) += rhs.getM(1);

        if (common::addOverflow(getM(2), rhs.getM(2), getM(2)))
            throw Exception("Decimal math overflow", ErrorCodes::DECIMAL_OVERFLOW);
        if constexpr (_level >= 3)
            if (common::addOverflow(getM(3), rhs.getM(3), getM(3)))
                throw Exception("Decimal math overflow", ErrorCodes::DECIMAL_OVERFLOW);
        if constexpr (_level >= 4)
            if (common::addOverflow(getM(4), rhs.getM(4), getM(4)))
                throw Exception("Decimal math overflow", ErrorCodes::DECIMAL_OVERFLOW);
    }

    void write(WriteBuffer & buf) const { writePODBinary(*this, buf); }
    void read(ReadBuffer & buf) { readPODBinary(*this, buf); }

    Float64 getPopulation(UInt32 scale) const
    {
        if (m0 == 0)
            return std::numeric_limits<Float64>::infinity();

        NativeType tmp;
        if (common::mulOverflow(getM(1), getM(1), tmp) ||
            common::subOverflow(getM(2), NativeType(tmp / m0), tmp))
            throw Exception("Decimal math overflow", ErrorCodes::DECIMAL_OVERFLOW);
        return std::max(Float64{}, convertFromDecimal<DataTypeDecimal<T>, DataTypeNumber<Float64>>(tmp / m0, scale));
    }

    Float64 getSample(UInt32 scale) const
    {
        if (m0 == 0)
            return std::numeric_limits<Float64>::quiet_NaN();
        if (m0 == 1)
            return std::numeric_limits<Float64>::infinity();

        NativeType tmp;
        if (common::mulOverflow(getM(1), getM(1), tmp) ||
            common::subOverflow(getM(2), NativeType(tmp / m0), tmp))
            throw Exception("Decimal math overflow", ErrorCodes::DECIMAL_OVERFLOW);
        return std::max(Float64{}, convertFromDecimal<DataTypeDecimal<T>, DataTypeNumber<Float64>>(tmp / (m0 - 1), scale));
    }

    Float64 getMoment3(UInt32 scale) const
    {
        if (m0 == 0)
            return std::numeric_limits<Float64>::infinity();

        NativeType tmp;
        if (common::mulOverflow(2 * getM(1), getM(1), tmp) ||
            common::subOverflow(3 * getM(2), NativeType(tmp / m0), tmp) ||
            common::mulOverflow(tmp, getM(1), tmp) ||
            common::subOverflow(getM(3), NativeType(tmp / m0), tmp))
            throw Exception("Decimal math overflow", ErrorCodes::DECIMAL_OVERFLOW);
        return convertFromDecimal<DataTypeDecimal<T>, DataTypeNumber<Float64>>(tmp / m0, scale);
    }

    Float64 getMoment4(UInt32 scale) const
    {
        if (m0 == 0)
            return std::numeric_limits<Float64>::infinity();

        NativeType tmp;
        if (common::mulOverflow(3 * getM(1), getM(1), tmp) ||
            common::subOverflow(6 * getM(2), NativeType(tmp / m0), tmp) ||
            common::mulOverflow(tmp, getM(1), tmp) ||
            common::subOverflow(4 * getM(3), NativeType(tmp / m0), tmp) ||
            common::mulOverflow(tmp, getM(1), tmp) ||
            common::subOverflow(getM(4), NativeType(tmp / m0), tmp))
            throw Exception("Decimal math overflow", ErrorCodes::DECIMAL_OVERFLOW);
        return convertFromDecimal<DataTypeDecimal<T>, DataTypeNumber<Float64>>(tmp / m0, scale);
    }
};

/**
    Calculating multivariate central moments
    Levels:
        level 2 (pop & samp): covar
    References:
        https://en.wikipedia.org/wiki/Moment_(mathematics)
*/
template <typename T>
struct CovarMoments
{
    T m0{};
    T x1{};
    T y1{};
    T xy{};

    void add(T x, T y)
    {
        ++m0;
        x1 += x;
        y1 += y;
        xy += x * y;
    }

    void merge(const CovarMoments & rhs)
    {
        m0 += rhs.m0;
        x1 += rhs.x1;
        y1 += rhs.y1;
        xy += rhs.xy;
    }

    void write(WriteBuffer & buf) const
    {
        writePODBinary(*this, buf);
    }

    void read(ReadBuffer & buf)
    {
        readPODBinary(*this, buf);
    }

    T NO_SANITIZE_UNDEFINED getPopulation() const
    {
        return (xy - x1 * y1 / m0) / m0;
    }

    T NO_SANITIZE_UNDEFINED getSample() const
    {
        if (m0 == 0)
            return std::numeric_limits<T>::quiet_NaN();
        return (xy - x1 * y1 / m0) / (m0 - 1);
    }
};

template <typename T>
struct CorrMoments
{
    T m0{};
    T x1{};
    T y1{};
    T xy{};
    T x2{};
    T y2{};

    void add(T x, T y)
    {
        ++m0;
        x1 += x;
        y1 += y;
        xy += x * y;
        x2 += x * x;
        y2 += y * y;
    }

    void merge(const CorrMoments & rhs)
    {
        m0 += rhs.m0;
        x1 += rhs.x1;
        y1 += rhs.y1;
        xy += rhs.xy;
        x2 += rhs.x2;
        y2 += rhs.y2;
    }

    void write(WriteBuffer & buf) const
    {
        writePODBinary(*this, buf);
    }

    void read(ReadBuffer & buf)
    {
        readPODBinary(*this, buf);
    }

    T NO_SANITIZE_UNDEFINED get() const
    {
        return (m0 * xy - x1 * y1) / sqrt((m0 * x2 - x1 * x1) * (m0 * y2 - y1 * y1));
    }
};


enum class StatisticsFunctionKind
{
    varPop, varSamp,
    stddevPop, stddevSamp,
    skewPop, skewSamp,
    kurtPop, kurtSamp,
    covarPop, covarSamp,
    corr
};


template <typename T, StatisticsFunctionKind _kind, size_t _level>
struct StatFuncOneArg
{
    using Type1 = T;
    using Type2 = T;
    using ResultType = std::conditional_t<std::is_same_v<T, Float32>, Float32, Float64>;
    using Data = std::conditional_t<IsDecimalNumber<T>, VarMomentsDecimal<Decimal128, _level>, VarMoments<ResultType, _level>>;

    static constexpr StatisticsFunctionKind kind = _kind;
    static constexpr UInt32 num_args = 1;
};

template <typename T1, typename T2, StatisticsFunctionKind _kind>
struct StatFuncTwoArg
{
    using Type1 = T1;
    using Type2 = T2;
    using ResultType = std::conditional_t<std::is_same_v<T1, T2> && std::is_same_v<T1, Float32>, Float32, Float64>;
    using Data = std::conditional_t<_kind == StatisticsFunctionKind::corr, CorrMoments<ResultType>, CovarMoments<ResultType>>;

    static constexpr StatisticsFunctionKind kind = _kind;
    static constexpr UInt32 num_args = 2;
};


template <typename StatFunc>
class AggregateFunctionVarianceSimple final
    : public IAggregateFunctionDataHelper<typename StatFunc::Data, AggregateFunctionVarianceSimple<StatFunc>>
{
public:
    using T1 = typename StatFunc::Type1;
    using T2 = typename StatFunc::Type2;
    using ColVecT1 = std::conditional_t<IsDecimalNumber<T1>, ColumnDecimal<T1>, ColumnVector<T1>>;
    using ColVecT2 = std::conditional_t<IsDecimalNumber<T2>, ColumnDecimal<T2>, ColumnVector<T2>>;
    using ResultType = typename StatFunc::ResultType;
    using ColVecResult = ColumnVector<ResultType>;

    AggregateFunctionVarianceSimple(const DataTypes & argument_types_)
        : IAggregateFunctionDataHelper<typename StatFunc::Data, AggregateFunctionVarianceSimple<StatFunc>>(argument_types_, {})
        , src_scale(0)
    {}

    AggregateFunctionVarianceSimple(const IDataType & data_type, const DataTypes & argument_types_)
        : IAggregateFunctionDataHelper<typename StatFunc::Data, AggregateFunctionVarianceSimple<StatFunc>>(argument_types_, {})
        , src_scale(getDecimalScale(data_type))
    {}

    String getName() const override
    {
        if constexpr (StatFunc::kind == StatisticsFunctionKind::varPop)
            return "varPop";
        if constexpr (StatFunc::kind == StatisticsFunctionKind::varSamp)
            return "varSamp";
        if constexpr (StatFunc::kind == StatisticsFunctionKind::stddevPop)
            return "stddevPop";
        if constexpr (StatFunc::kind == StatisticsFunctionKind::stddevSamp)
            return "stddevSamp";
        if constexpr (StatFunc::kind == StatisticsFunctionKind::skewPop)
            return "skewPop";
        if constexpr (StatFunc::kind == StatisticsFunctionKind::skewSamp)
            return "skewSamp";
        if constexpr (StatFunc::kind == StatisticsFunctionKind::kurtPop)
            return "kurtPop";
        if constexpr (StatFunc::kind == StatisticsFunctionKind::kurtSamp)
            return "kurtSamp";
        if constexpr (StatFunc::kind == StatisticsFunctionKind::covarPop)
            return "covarPop";
        if constexpr (StatFunc::kind == StatisticsFunctionKind::covarSamp)
            return "covarSamp";
        if constexpr (StatFunc::kind == StatisticsFunctionKind::corr)
            return "corr";
        __builtin_unreachable();
    }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeNumber<ResultType>>();
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        if constexpr (StatFunc::num_args == 2)
            this->data(place).add(
                static_cast<const ColVecT1 &>(*columns[0]).getData()[row_num],
                static_cast<const ColVecT2 &>(*columns[1]).getData()[row_num]);
        else
            this->data(place).add(
                static_cast<const ColVecT1 &>(*columns[0]).getData()[row_num]);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).read(buf);
    }

    void insertResultInto(AggregateDataPtr place, IColumn & to, Arena *) const override
    {
        const auto & data = this->data(place);
        auto & dst = static_cast<ColVecResult &>(to).getData();

        if constexpr (IsDecimalNumber<T1>)
        {
            if constexpr (StatFunc::kind == StatisticsFunctionKind::varPop)
                dst.push_back(data.getPopulation(src_scale * 2));
            if constexpr (StatFunc::kind == StatisticsFunctionKind::varSamp)
                dst.push_back(data.getSample(src_scale * 2));
            if constexpr (StatFunc::kind == StatisticsFunctionKind::stddevPop)
                dst.push_back(sqrt(data.getPopulation(src_scale * 2)));
            if constexpr (StatFunc::kind == StatisticsFunctionKind::stddevSamp)
                dst.push_back(sqrt(data.getSample(src_scale * 2)));
            if constexpr (StatFunc::kind == StatisticsFunctionKind::skewPop)
            {
                Float64 var_value = data.getPopulation(src_scale * 2);

                if (var_value > 0)
                    dst.push_back(data.getMoment3(src_scale * 3) / pow(var_value, 1.5));
                else
                    dst.push_back(std::numeric_limits<Float64>::quiet_NaN());
            }
            if constexpr (StatFunc::kind == StatisticsFunctionKind::skewSamp)
            {
                Float64 var_value = data.getSample(src_scale * 2);

                if (var_value > 0)
                    dst.push_back(data.getMoment3(src_scale * 3) / pow(var_value, 1.5));
                else
                    dst.push_back(std::numeric_limits<Float64>::quiet_NaN());
            }
            if constexpr (StatFunc::kind == StatisticsFunctionKind::kurtPop)
            {
                Float64 var_value = data.getPopulation(src_scale * 2);

                if (var_value > 0)
                    dst.push_back(data.getMoment4(src_scale * 4) / pow(var_value, 2));
                else
                    dst.push_back(std::numeric_limits<Float64>::quiet_NaN());
            }
            if constexpr (StatFunc::kind == StatisticsFunctionKind::kurtSamp)
            {
                Float64 var_value = data.getSample(src_scale * 2);

                if (var_value > 0)
                    dst.push_back(data.getMoment4(src_scale * 4) / pow(var_value, 2));
                else
                    dst.push_back(std::numeric_limits<Float64>::quiet_NaN());
            }
        }
        else
        {
            if constexpr (StatFunc::kind == StatisticsFunctionKind::varPop)
                dst.push_back(data.getPopulation());
            if constexpr (StatFunc::kind == StatisticsFunctionKind::varSamp)
                dst.push_back(data.getSample());
            if constexpr (StatFunc::kind == StatisticsFunctionKind::stddevPop)
                dst.push_back(sqrt(data.getPopulation()));
            if constexpr (StatFunc::kind == StatisticsFunctionKind::stddevSamp)
                dst.push_back(sqrt(data.getSample()));
            if constexpr (StatFunc::kind == StatisticsFunctionKind::skewPop)
            {
                ResultType var_value = data.getPopulation();

                if (var_value > 0)
                    dst.push_back(data.getMoment3() / pow(var_value, 1.5));
                else
                    dst.push_back(std::numeric_limits<ResultType>::quiet_NaN());
            }
            if constexpr (StatFunc::kind == StatisticsFunctionKind::skewSamp)
            {
                ResultType var_value = data.getSample();

                if (var_value > 0)
                    dst.push_back(data.getMoment3() / pow(var_value, 1.5));
                else
                    dst.push_back(std::numeric_limits<ResultType>::quiet_NaN());
            }
            if constexpr (StatFunc::kind == StatisticsFunctionKind::kurtPop)
            {
                ResultType var_value = data.getPopulation();

                if (var_value > 0)
                    dst.push_back(data.getMoment4() / pow(var_value, 2));
                else
                    dst.push_back(std::numeric_limits<ResultType>::quiet_NaN());
            }
            if constexpr (StatFunc::kind == StatisticsFunctionKind::kurtSamp)
            {
                ResultType var_value = data.getSample();

                if (var_value > 0)
                    dst.push_back(data.getMoment4() / pow(var_value, 2));
                else
                    dst.push_back(std::numeric_limits<ResultType>::quiet_NaN());
            }
            if constexpr (StatFunc::kind == StatisticsFunctionKind::covarPop)
                dst.push_back(data.getPopulation());
            if constexpr (StatFunc::kind == StatisticsFunctionKind::covarSamp)
                dst.push_back(data.getSample());
            if constexpr (StatFunc::kind == StatisticsFunctionKind::corr)
                dst.push_back(data.get());
        }
    }

private:
    UInt32 src_scale;
};


template <typename T> using AggregateFunctionVarPopSimple = AggregateFunctionVarianceSimple<StatFuncOneArg<T, StatisticsFunctionKind::varPop, 2>>;
template <typename T> using AggregateFunctionVarSampSimple = AggregateFunctionVarianceSimple<StatFuncOneArg<T, StatisticsFunctionKind::varSamp, 2>>;
template <typename T> using AggregateFunctionStddevPopSimple = AggregateFunctionVarianceSimple<StatFuncOneArg<T, StatisticsFunctionKind::stddevPop, 2>>;
template <typename T> using AggregateFunctionStddevSampSimple = AggregateFunctionVarianceSimple<StatFuncOneArg<T, StatisticsFunctionKind::stddevSamp, 2>>;
template <typename T> using AggregateFunctionSkewPopSimple = AggregateFunctionVarianceSimple<StatFuncOneArg<T, StatisticsFunctionKind::skewPop, 3>>;
template <typename T> using AggregateFunctionSkewSampSimple = AggregateFunctionVarianceSimple<StatFuncOneArg<T, StatisticsFunctionKind::skewSamp, 3>>;
template <typename T> using AggregateFunctionKurtPopSimple = AggregateFunctionVarianceSimple<StatFuncOneArg<T, StatisticsFunctionKind::kurtPop, 4>>;
template <typename T> using AggregateFunctionKurtSampSimple = AggregateFunctionVarianceSimple<StatFuncOneArg<T, StatisticsFunctionKind::kurtSamp, 4>>;
template <typename T1, typename T2> using AggregateFunctionCovarPopSimple = AggregateFunctionVarianceSimple<StatFuncTwoArg<T1, T2, StatisticsFunctionKind::covarPop>>;
template <typename T1, typename T2> using AggregateFunctionCovarSampSimple = AggregateFunctionVarianceSimple<StatFuncTwoArg<T1, T2, StatisticsFunctionKind::covarSamp>>;
template <typename T1, typename T2> using AggregateFunctionCorrSimple = AggregateFunctionVarianceSimple<StatFuncTwoArg<T1, T2, StatisticsFunctionKind::corr>>;

}
