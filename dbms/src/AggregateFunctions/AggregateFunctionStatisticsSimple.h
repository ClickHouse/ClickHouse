#pragma once

#include <cmath>

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <AggregateFunctions/IAggregateFunction.h>

#include <DataTypes/DataTypesNumber.h>
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

enum class VarianceMode
{
    Population,
    Sample
};

enum class VariancePower
{
    Original,
    Sqrt
};


template <typename T>
struct VarMoments
{
    T m0{};
    T m1{};
    T m2{};

    void add(T x)
    {
        ++m0;
        m1 += x;
        m2 += x * x;
    }

    void merge(const VarMoments & rhs)
    {
        m0 += rhs.m0;
        m1 += rhs.m1;
        m2 += rhs.m2;
    }

    void write(WriteBuffer & buf) const
    {
        writePODBinary(*this, buf);
    }

    void read(ReadBuffer & buf)
    {
        readPODBinary(*this, buf);
    }

    template <VarianceMode mode, VariancePower power>
    T get() const
    {
        if (m0 == 0 && mode == VarianceMode::Sample)
            return std::numeric_limits<T>::quiet_NaN();

        T res = (m2 - m1 * m1 / m0) / (m0 - (mode == VarianceMode::Sample));
        return power == VariancePower::Original ? res : sqrt(res);
    }
};

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

    template <VarianceMode mode>
    T get() const
    {
        if (m0 == 0 && mode == VarianceMode::Sample)
            return std::numeric_limits<T>::quiet_NaN();

        return (xy - x1 * y1 / m0) / (m0 - (mode == VarianceMode::Sample));
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

    T get() const
    {
        return (m0 * xy - x1 * y1) / sqrt((m0 * x2 - x1 * x1) * (m0 * y2 - y1 * y1));
    }
};


enum class StatisticsFunctionKind
{
    varPop, varSamp,
    stddevPop, stddevSamp,
    covarPop, covarSamp,
    corr
};


template <typename T1, typename T2>
using VarianceCalcType = std::conditional_t<std::is_same_v<T1, Float32> && std::is_same_v<T2, Float32>, Float32, Float64>;


template <typename T1, typename T2, typename Data, StatisticsFunctionKind Kind>
class AggregateFunctionVarianceSimple final
    : public IAggregateFunctionDataHelper<Data, AggregateFunctionVarianceSimple<T1, T2, Data, Kind>>
{
    using ResultType = VarianceCalcType<T1, T2>;

public:
    String getName() const override
    {
        switch (Kind)
        {
            case StatisticsFunctionKind::varPop: return "varPop";
            case StatisticsFunctionKind::varSamp: return "varSamp";
            case StatisticsFunctionKind::stddevPop: return "stddevPop";
            case StatisticsFunctionKind::stddevSamp: return "stddevSamp";
            case StatisticsFunctionKind::covarPop: return "covarPop";
            case StatisticsFunctionKind::covarSamp: return "covarSamp";
            case StatisticsFunctionKind::corr: return "corr";
        }
    }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeNumber<ResultType>>();
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        if constexpr (Kind == StatisticsFunctionKind::covarPop || Kind == StatisticsFunctionKind::covarSamp || Kind == StatisticsFunctionKind::corr)
            this->data(place).add(
                static_cast<const ColumnVector<T1> &>(*columns[0]).getData()[row_num],
                static_cast<const ColumnVector<T2> &>(*columns[1]).getData()[row_num]);
        else
            this->data(place).add(
                static_cast<const ColumnVector<T1> &>(*columns[0]).getData()[row_num]);
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

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        const auto & data = this->data(place);
        auto & dst = static_cast<ColumnVector<ResultType> &>(to).getData();

             if constexpr (Kind == StatisticsFunctionKind::varPop) dst.push_back(data.template get<VarianceMode::Population, VariancePower::Original>());
        else if constexpr (Kind == StatisticsFunctionKind::varSamp) dst.push_back(data.template get<VarianceMode::Sample, VariancePower::Original>());
        else if constexpr (Kind == StatisticsFunctionKind::stddevPop) dst.push_back(data.template get<VarianceMode::Population, VariancePower::Sqrt>());
        else if constexpr (Kind == StatisticsFunctionKind::stddevSamp) dst.push_back(data.template get<VarianceMode::Sample, VariancePower::Sqrt>());
        else if constexpr (Kind == StatisticsFunctionKind::covarPop) dst.push_back(data.template get<VarianceMode::Population>());
        else if constexpr (Kind == StatisticsFunctionKind::covarSamp) dst.push_back(data.template get<VarianceMode::Sample>());
        else if constexpr (Kind == StatisticsFunctionKind::corr) dst.push_back(data.get());
    }

    const char * getHeaderFilePath() const override { return __FILE__; }
};


template <typename T> using AggregateFunctionVarPopSimple = AggregateFunctionVarianceSimple<T, T, VarMoments<VarianceCalcType<T, T>>, StatisticsFunctionKind::varPop>;
template <typename T> using AggregateFunctionVarSampSimple = AggregateFunctionVarianceSimple<T, T, VarMoments<VarianceCalcType<T, T>>, StatisticsFunctionKind::varSamp>;
template <typename T> using AggregateFunctionStddevPopSimple = AggregateFunctionVarianceSimple<T, T, VarMoments<VarianceCalcType<T, T>>, StatisticsFunctionKind::stddevPop>;
template <typename T> using AggregateFunctionStddevSampSimple = AggregateFunctionVarianceSimple<T, T, VarMoments<VarianceCalcType<T, T>>, StatisticsFunctionKind::stddevSamp>;
template <typename T1, typename T2> using AggregateFunctionCovarPopSimple = AggregateFunctionVarianceSimple<T1, T2, CovarMoments<VarianceCalcType<T1, T2>>, StatisticsFunctionKind::covarPop>;
template <typename T1, typename T2> using AggregateFunctionCovarSampSimple = AggregateFunctionVarianceSimple<T1, T2, CovarMoments<VarianceCalcType<T1, T2>>, StatisticsFunctionKind::covarSamp>;
template <typename T1, typename T2> using AggregateFunctionCorrSimple = AggregateFunctionVarianceSimple<T1, T2, CorrMoments<VarianceCalcType<T1, T2>>, StatisticsFunctionKind::corr>;

}
