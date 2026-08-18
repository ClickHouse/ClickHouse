#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <DataTypes/DataTypesNumber.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnsNumber.h>
#include <Common/assert_cast.h>

#include <cmath>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

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
struct AggregateFunctionVarianceData
{
    void update(const IColumn & column, size_t row_num)
    {
        Float64 val = column.getFloat64(row_num);
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

    UInt64 count = 0;
    Float64 mean = 0.0;
    Float64 m2 = 0.0;
};

enum class VarKind : uint8_t
{
    varSampStable,
    stddevSampStable,
    varPopStable,
    stddevPopStable,
};

/** The main code for the implementation of varSamp, stddevSamp, varPop, stddevPop.
  */
class AggregateFunctionVariance final
    : public IAggregateFunctionDataHelper<AggregateFunctionVarianceData, AggregateFunctionVariance>
{
private:
    VarKind kind;

    static Float64 getVarSamp(Float64 m2, UInt64 count)
    {
        if (count < 2)
            return std::numeric_limits<Float64>::infinity();
        return m2 / (count - 1);
    }

    static Float64 getStddevSamp(Float64 m2, UInt64 count)
    {
        return sqrt(getVarSamp(m2, count));
    }

    static Float64 getVarPop(Float64 m2, UInt64 count)
    {
        if (count == 0)
            return std::numeric_limits<Float64>::infinity();
        if (count == 1)
            return 0.0;
        return m2 / count;
    }

    static Float64 getStddevPop(Float64 m2, UInt64 count)
    {
        return sqrt(getVarPop(m2, count));
    }

    Float64 getResult(ConstAggregateDataPtr __restrict place) const
    {
        const auto & dt = data(place);
        switch (kind)
        {
            case VarKind::varSampStable: return getVarSamp(dt.m2, dt.count);
            case VarKind::stddevSampStable: return getStddevSamp(dt.m2, dt.count);
            case VarKind::varPopStable: return getVarPop(dt.m2, dt.count);
            case VarKind::stddevPopStable: return getStddevPop(dt.m2, dt.count);
        }
    }

public:
    explicit AggregateFunctionVariance(VarKind kind_, const DataTypePtr & arg)
        : IAggregateFunctionDataHelper<AggregateFunctionVarianceData, AggregateFunctionVariance>({arg}, {}, std::make_shared<DataTypeFloat64>()),
        kind(kind_)
    {
    }

    String getName() const override
    {
        switch (kind)
        {
            case VarKind::varSampStable: return "varSampStable";
            case VarKind::stddevSampStable: return "stddevSampStable";
            case VarKind::varPopStable: return "varPopStable";
            case VarKind::stddevPopStable: return "stddevPopStable";
        }
    }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        data(place).update(*columns[0], row_num);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        data(place).mergeWith(data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        data(place).deserialize(buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        assert_cast<ColumnFloat64 &>(to).getData().push_back(getResult(place));
    }
};


/** If `compute_marginal_moments` flag is set this class provides the successor
  * CovarianceData support of marginal moments for calculating the correlation.
  */
template <bool compute_marginal_moments>
struct BaseCovarianceData
{
    void incrementMarginalMoments(Float64, Float64) {}
    void mergeWith(const BaseCovarianceData &) {}
    void serialize(WriteBuffer &) const {}
    void deserialize(const ReadBuffer &) {}
};

template <>
struct BaseCovarianceData<true>
{
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

    Float64 left_m2 = 0.0;
    Float64 right_m2 = 0.0;
};

/** Parallel and incremental algorithm for calculating covariance.
  * Source: "Numerically Stable, Single-Pass, Parallel Statistics Algorithms"
  * (J. Bennett et al., Sandia National Laboratories,
  *  2009 IEEE International Conference on Cluster Computing)
  */
template <bool compute_marginal_moments>
struct CovarianceData : public BaseCovarianceData<compute_marginal_moments>
{
    using Base = BaseCovarianceData<compute_marginal_moments>;

    void update(const IColumn & column_left, const IColumn & column_right, size_t row_num)
    {
        Float64 left_val = column_left.getFloat64(row_num);
        Float64 left_delta = left_val - left_mean;

        Float64 right_val = column_right.getFloat64(row_num);
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

    UInt64 count = 0;
    Float64 left_mean = 0.0;
    Float64 right_mean = 0.0;
    Float64 co_moment = 0.0;
};

enum class CovarKind : uint8_t
{
    covarSampStable,
    covarPopStable,
    corrStable,
};

template <bool compute_marginal_moments>
class AggregateFunctionCovariance final
    : public IAggregateFunctionDataHelper<
        CovarianceData<compute_marginal_moments>,
        AggregateFunctionCovariance<compute_marginal_moments>>
{
private:
    CovarKind kind;

    static Float64 getCovarSamp(Float64 co_moment, UInt64 count)
    {
        if (count < 2)
            return std::numeric_limits<Float64>::infinity();
        return co_moment / (count - 1);
    }

    static Float64 getCovarPop(Float64 co_moment, UInt64 count)
    {
        if (count == 0)
            return std::numeric_limits<Float64>::infinity();
        if (count == 1)
            return 0.0;
        return co_moment / count;
    }

    static Float64 getCorr(Float64 co_moment, Float64 left_m2, Float64 right_m2, UInt64 count)
    {
        if (count < 2)
            return std::numeric_limits<Float64>::infinity();
        return co_moment / sqrt(left_m2 * right_m2);
    }

    Float64 getResult(ConstAggregateDataPtr __restrict place) const
    {
        const auto & data = this->data(place);
        switch (kind)
        {
            case CovarKind::covarSampStable: return getCovarSamp(data.co_moment, data.count);
            case CovarKind::covarPopStable: return getCovarPop(data.co_moment, data.count);

            case CovarKind::corrStable:
                if constexpr (compute_marginal_moments)
                    return getCorr(data.co_moment, data.left_m2, data.right_m2, data.count);
                else
                    return 0;
        }
    }

public:
    explicit AggregateFunctionCovariance(CovarKind kind_, const DataTypes & args) : IAggregateFunctionDataHelper<
        CovarianceData<compute_marginal_moments>,
        AggregateFunctionCovariance<compute_marginal_moments>>(args, {}, std::make_shared<DataTypeFloat64>()),
        kind(kind_)
    {
    }

    String getName() const override
    {
        switch (kind)
        {
            case CovarKind::covarSampStable: return "covarSampStable";
            case CovarKind::covarPopStable: return "covarPopStable";
            case CovarKind::corrStable: return "corrStable";
        }
    }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        this->data(place).update(*columns[0], *columns[1], row_num);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).mergeWith(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        this->data(place).deserialize(buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        assert_cast<ColumnFloat64 &>(to).getData().push_back(getResult(place));
    }
};


template <template <typename> typename FunctionTemplate>
AggregateFunctionPtr createAggregateFunctionStatisticsUnary(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    AggregateFunctionPtr res(createWithNumericType<FunctionTemplate>(*argument_types[0], argument_types[0]));

    if (!res)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Illegal type {} of argument for aggregate function {}", argument_types[0]->getName(), name);

    return res;
}

template <template <typename, typename> typename FunctionTemplate>
AggregateFunctionPtr createAggregateFunctionStatisticsBinary(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    assertBinary(name, argument_types);

    AggregateFunctionPtr res(createWithTwoBasicNumericTypes<FunctionTemplate>(*argument_types[0], *argument_types[1], argument_types));
    if (!res)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal types {} and {} of arguments for aggregate function {}",
            argument_types[0]->getName(), argument_types[1]->getName(), name);

    return res;
}

}

void registerAggregateFunctionsStatisticsStable(AggregateFunctionFactory & factory)
{
    factory.registerFunction("varSampStable", [](const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
    {
        assertNoParameters(name, parameters);
        assertUnary(name, argument_types);
        return std::make_shared<AggregateFunctionVariance>(VarKind::varSampStable, argument_types[0]);
    });

    factory.registerFunction("varPopStable", [](const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
    {
        assertNoParameters(name, parameters);
        assertUnary(name, argument_types);
        return std::make_shared<AggregateFunctionVariance>(VarKind::varPopStable, argument_types[0]);
    });

    factory.registerFunction("stddevSampStable", [](const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
    {
        assertNoParameters(name, parameters);
        assertUnary(name, argument_types);
        return std::make_shared<AggregateFunctionVariance>(VarKind::stddevSampStable, argument_types[0]);
    });

    factory.registerFunction("stddevPopStable", [](const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
    {
        assertNoParameters(name, parameters);
        assertUnary(name, argument_types);
        return std::make_shared<AggregateFunctionVariance>(VarKind::stddevPopStable, argument_types[0]);
    });

    factory.registerFunction("covarSampStable", [](const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
    {
        assertNoParameters(name, parameters);
        assertBinary(name, argument_types);
        return std::make_shared<AggregateFunctionCovariance<false>>(CovarKind::covarSampStable, argument_types);
    });

    factory.registerFunction("covarPopStable", [](const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
    {
        assertNoParameters(name, parameters);
        assertBinary(name, argument_types);
        return std::make_shared<AggregateFunctionCovariance<false>>(CovarKind::covarPopStable, argument_types);
    });

    factory.registerFunction("corrStable", [](const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
    {
        assertNoParameters(name, parameters);
        assertBinary(name, argument_types);
        return std::make_shared<AggregateFunctionCovariance<true>>(CovarKind::corrStable, argument_types);
    });
}

}
