#pragma once

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <DataTypes/DataTypesNumber.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnsNumber.h>
#include <Common/assert_cast.h>

#include <cmath>


namespace DB
{

namespace
{

/// This function returns true if both values are large and comparable.
/// It is used to calculate the mean value by merging two sources.
/// It means that if the sizes of both sources are large and comparable, then we must apply a special
///  formula guaranteeing more stability.
bool areComparable(UInt64 a, UInt64 b)
{
    const Float64 sensitivity = 0.001;
    const UInt64 threshold = 10000;

    if ((a == 0) || (b == 0))
        return false;

    auto res = std::minmax(a, b);
    return (((1 - static_cast<Float64>(res.first) / res.second) < sensitivity) && (res.first > threshold));
}

}

/** Statistical aggregate functions
  * varSamp - sample variance
  * stddevSamp - mean sample quadratic deviation
  * varPop - variance
  * stddevPop - standard deviation
  * covarSamp - selective covariance
  * covarPop - covariance
  * corr - correlation
  */

/** Parallel and incremental algorithm for calculating variance.
  * Source: "Updating formulae and a pairwise algorithm for computing sample variances"
  * (Chan et al., Stanford University, 12.1979)
  */
template <typename T, typename Op>
class AggregateFunctionVarianceData
{
public:
    void update(const IColumn & column, size_t row_num)
    {
        T received = assert_cast<const ColumnVector<T> &>(column).getData()[row_num];
        Float64 val = static_cast<Float64>(received);
        Float64 delta = val - mean;

        ++count;
        mean += delta / count;
        m2 += delta * (val - mean);
    }

    void mergeWith(const AggregateFunctionVarianceData & source)
    {
        UInt64 total_count = count + source.count;
        if (total_count == 0)
            return;

        Float64 factor = static_cast<Float64>(count * source.count) / total_count;
        Float64 delta = mean - source.mean;

        if (areComparable(count, source.count))
            mean = (source.count * source.mean + count * mean) / total_count;
        else
            mean = source.mean + delta * (static_cast<Float64>(count) / total_count);

        m2 += source.m2 + delta * delta * factor;
        count = total_count;
    }

    void serialize(WriteBuffer & buf) const
    {
        writeVarUInt(count, buf);
        writeBinary(mean, buf);
        writeBinary(m2, buf);
    }

    void deserialize(ReadBuffer & buf)
    {
        readVarUInt(count, buf);
        readBinary(mean, buf);
        readBinary(m2, buf);
    }

    void publish(IColumn & to) const
    {
        assert_cast<ColumnFloat64 &>(to).getData().push_back(Op::apply(m2, count));
    }

private:
    UInt64 count = 0;
    Float64 mean = 0.0;
    Float64 m2 = 0.0;
};

/** The main code for the implementation of varSamp, stddevSamp, varPop, stddevPop.
  */
template <typename T, typename Op>
class AggregateFunctionVariance final
    : public IAggregateFunctionDataHelper<AggregateFunctionVarianceData<T, Op>, AggregateFunctionVariance<T, Op>>
{
public:
    AggregateFunctionVariance(const DataTypePtr & arg)
        : IAggregateFunctionDataHelper<AggregateFunctionVarianceData<T, Op>, AggregateFunctionVariance<T, Op>>({arg}, {}) {}

    String getName() const override { return Op::name; }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeFloat64>();
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        this->data(place).update(*columns[0], row_num);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).mergeWith(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).deserialize(buf);
    }

    void insertResultInto(AggregateDataPtr place, IColumn & to, Arena *) const override
    {
        this->data(place).publish(to);
    }
};

/** Implementing the varSamp function.
  */
struct AggregateFunctionVarSampImpl
{
    static constexpr auto name = "varSampStable";

    static inline Float64 apply(Float64 m2, UInt64 count)
    {
        if (count < 2)
            return std::numeric_limits<Float64>::infinity();
        else
            return m2 / (count - 1);
    }
};

/** Implementing the stddevSamp function.
  */
struct AggregateFunctionStdDevSampImpl
{
    static constexpr auto name = "stddevSampStable";

    static inline Float64 apply(Float64 m2, UInt64 count)
    {
        return sqrt(AggregateFunctionVarSampImpl::apply(m2, count));
    }
};

/** Implementing the varPop function.
  */
struct AggregateFunctionVarPopImpl
{
    static constexpr auto name = "varPopStable";

    static inline Float64 apply(Float64 m2, UInt64 count)
    {
        if (count == 0)
            return std::numeric_limits<Float64>::infinity();
        else if (count == 1)
            return 0.0;
        else
            return m2 / count;
    }
};

/** Implementing the stddevPop function.
  */
struct AggregateFunctionStdDevPopImpl
{
    static constexpr auto name = "stddevPopStable";

    static inline Float64 apply(Float64 m2, UInt64 count)
    {
        return sqrt(AggregateFunctionVarPopImpl::apply(m2, count));
    }
};

/** If `compute_marginal_moments` flag is set this class provides the successor
  * CovarianceData support of marginal moments for calculating the correlation.
  */
template <bool compute_marginal_moments>
class BaseCovarianceData
{
protected:
    void incrementMarginalMoments(Float64, Float64) {}
    void mergeWith(const BaseCovarianceData &) {}
    void serialize(WriteBuffer &) const {}
    void deserialize(const ReadBuffer &) {}
};

template <>
class BaseCovarianceData<true>
{
protected:
    void incrementMarginalMoments(Float64 left_incr, Float64 right_incr)
    {
        left_m2 += left_incr;
        right_m2 += right_incr;
    }

    void mergeWith(const BaseCovarianceData & source)
    {
        left_m2 += source.left_m2;
        right_m2 += source.right_m2;
    }

    void serialize(WriteBuffer & buf) const
    {
        writeBinary(left_m2, buf);
        writeBinary(right_m2, buf);
    }

    void deserialize(ReadBuffer & buf)
    {
        readBinary(left_m2, buf);
        readBinary(right_m2, buf);
    }

protected:
    Float64 left_m2 = 0.0;
    Float64 right_m2 = 0.0;
};

/** Parallel and incremental algorithm for calculating covariance.
  * Source: "Numerically Stable, Single-Pass, Parallel Statistics Algorithms"
  * (J. Bennett et al., Sandia National Laboratories,
  *  2009 IEEE International Conference on Cluster Computing)
  */
template <typename T, typename U, typename Op, bool compute_marginal_moments>
class CovarianceData : public BaseCovarianceData<compute_marginal_moments>
{
private:
    using Base = BaseCovarianceData<compute_marginal_moments>;

public:
    void update(const IColumn & column_left, const IColumn & column_right, size_t row_num)
    {
        T left_received = assert_cast<const ColumnVector<T> &>(column_left).getData()[row_num];
        Float64 left_val = static_cast<Float64>(left_received);
        Float64 left_delta = left_val - left_mean;

        U right_received = assert_cast<const ColumnVector<U> &>(column_right).getData()[row_num];
        Float64 right_val = static_cast<Float64>(right_received);
        Float64 right_delta = right_val - right_mean;

        Float64 old_right_mean = right_mean;

        ++count;

        left_mean += left_delta / count;
        right_mean += right_delta / count;
        co_moment += (left_val - left_mean) * (right_val - old_right_mean);

        /// Update the marginal moments, if any.
        if (compute_marginal_moments)
        {
            Float64 left_incr = left_delta * (left_val - left_mean);
            Float64 right_incr = right_delta * (right_val - right_mean);
            Base::incrementMarginalMoments(left_incr, right_incr);
        }
    }

    void mergeWith(const CovarianceData & source)
    {
        UInt64 total_count = count + source.count;
        if (total_count == 0)
            return;

        Float64 factor = static_cast<Float64>(count * source.count) / total_count;
        Float64 left_delta = left_mean - source.left_mean;
        Float64 right_delta = right_mean - source.right_mean;

        if (areComparable(count, source.count))
        {
            left_mean = (source.count * source.left_mean + count * left_mean) / total_count;
            right_mean = (source.count * source.right_mean + count * right_mean) / total_count;
        }
        else
        {
            left_mean = source.left_mean + left_delta * (static_cast<Float64>(count) / total_count);
            right_mean = source.right_mean + right_delta * (static_cast<Float64>(count) / total_count);
        }

        co_moment += source.co_moment + left_delta * right_delta * factor;
        count = total_count;

        /// Update the marginal moments, if any.
        if (compute_marginal_moments)
        {
            Float64 left_incr = left_delta * left_delta * factor;
            Float64 right_incr = right_delta * right_delta * factor;
            Base::mergeWith(source);
            Base::incrementMarginalMoments(left_incr, right_incr);
        }
    }

    void serialize(WriteBuffer & buf) const
    {
        writeVarUInt(count, buf);
        writeBinary(left_mean, buf);
        writeBinary(right_mean, buf);
        writeBinary(co_moment, buf);
        Base::serialize(buf);
    }

    void deserialize(ReadBuffer & buf)
    {
        readVarUInt(count, buf);
        readBinary(left_mean, buf);
        readBinary(right_mean, buf);
        readBinary(co_moment, buf);
        Base::deserialize(buf);
    }

    void publish(IColumn & to) const
    {
        if constexpr (compute_marginal_moments)
            assert_cast<ColumnFloat64 &>(to).getData().push_back(Op::apply(co_moment, Base::left_m2, Base::right_m2, count));
        else
            assert_cast<ColumnFloat64 &>(to).getData().push_back(Op::apply(co_moment, count));
    }

private:
    UInt64 count = 0;
    Float64 left_mean = 0.0;
    Float64 right_mean = 0.0;
    Float64 co_moment = 0.0;
};

template <typename T, typename U, typename Op, bool compute_marginal_moments = false>
class AggregateFunctionCovariance final
    : public IAggregateFunctionDataHelper<
        CovarianceData<T, U, Op, compute_marginal_moments>,
        AggregateFunctionCovariance<T, U, Op, compute_marginal_moments>>
{
public:
    AggregateFunctionCovariance(const DataTypes & args) : IAggregateFunctionDataHelper<
        CovarianceData<T, U, Op, compute_marginal_moments>,
        AggregateFunctionCovariance<T, U, Op, compute_marginal_moments>>(args, {}) {}

    String getName() const override { return Op::name; }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeFloat64>();
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        this->data(place).update(*columns[0], *columns[1], row_num);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).mergeWith(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).deserialize(buf);
    }

    void insertResultInto(AggregateDataPtr place, IColumn & to, Arena *) const override
    {
        this->data(place).publish(to);
    }
};

/** Implementing the covarSamp function.
  */
struct AggregateFunctionCovarSampImpl
{
    static constexpr auto name = "covarSampStable";

    static inline Float64 apply(Float64 co_moment, UInt64 count)
    {
        if (count < 2)
            return std::numeric_limits<Float64>::infinity();
        else
            return co_moment / (count - 1);
    }
};

/** Implementing the covarPop function.
  */
struct AggregateFunctionCovarPopImpl
{
    static constexpr auto name = "covarPopStable";

    static inline Float64 apply(Float64 co_moment, UInt64 count)
    {
        if (count == 0)
            return std::numeric_limits<Float64>::infinity();
        else if (count == 1)
            return 0.0;
        else
            return co_moment / count;
    }
};

/** `corr` function implementation.
  */
struct AggregateFunctionCorrImpl
{
    static constexpr auto name = "corrStable";

    static inline Float64 apply(Float64 co_moment, Float64 left_m2, Float64 right_m2, UInt64 count)
    {
        if (count < 2)
            return std::numeric_limits<Float64>::infinity();
        else
            return co_moment / sqrt(left_m2 * right_m2);
    }
};

template <typename T>
using AggregateFunctionVarSampStable = AggregateFunctionVariance<T, AggregateFunctionVarSampImpl>;

template <typename T>
using AggregateFunctionStddevSampStable = AggregateFunctionVariance<T, AggregateFunctionStdDevSampImpl>;

template <typename T>
using AggregateFunctionVarPopStable = AggregateFunctionVariance<T, AggregateFunctionVarPopImpl>;

template <typename T>
using AggregateFunctionStddevPopStable = AggregateFunctionVariance<T, AggregateFunctionStdDevPopImpl>;

template <typename T, typename U>
using AggregateFunctionCovarSampStable = AggregateFunctionCovariance<T, U, AggregateFunctionCovarSampImpl>;

template <typename T, typename U>
using AggregateFunctionCovarPopStable = AggregateFunctionCovariance<T, U, AggregateFunctionCovarPopImpl>;

template <typename T, typename U>
using AggregateFunctionCorrStable = AggregateFunctionCovariance<T, U, AggregateFunctionCorrImpl, true>;

}
