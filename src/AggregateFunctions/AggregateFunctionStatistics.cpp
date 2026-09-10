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
    return (((1 - static_cast<Float64>(res.first) / static_cast<Float64>(res.second)) < sensitivity) && (static_cast<Float64>(res.first) > threshold));
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
        mean += delta / static_cast<Float64>(count);
        m2 += delta * (val - mean);
    }

    void mergeWith(const AggregateFunctionVarianceData & source)
    {
        UInt64 total_count = count + source.count;
        if (total_count == 0)
            return;

        Float64 factor = static_cast<Float64>(count * source.count) / static_cast<Float64>(total_count);
        Float64 delta = mean - source.mean;

        if (areComparable(count, source.count))
            mean = (static_cast<Float64>(source.count) * source.mean + static_cast<Float64>(count) * mean) / static_cast<Float64>(total_count);
        else
            mean = source.mean + delta * (static_cast<Float64>(count) / static_cast<Float64>(total_count));

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
        return m2 / static_cast<Float64>(count - 1);
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
        return m2 / static_cast<Float64>(count);
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

        left_mean += left_delta / static_cast<Float64>(count);
        right_mean += right_delta / static_cast<Float64>(count);
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

        Float64 factor = static_cast<Float64>(count * source.count) / static_cast<Float64>(total_count);
        Float64 left_delta = left_mean - source.left_mean;
        Float64 right_delta = right_mean - source.right_mean;

        if (areComparable(count, source.count))
        {
            left_mean = (static_cast<Float64>(source.count) * source.left_mean + static_cast<Float64>(count) * left_mean) / static_cast<Float64>(total_count);
            right_mean = (static_cast<Float64>(source.count) * source.right_mean + static_cast<Float64>(count) * right_mean) / static_cast<Float64>(total_count);
        }
        else
        {
            left_mean = source.left_mean + left_delta * (static_cast<Float64>(count) / static_cast<Float64>(total_count));
            right_mean = source.right_mean + right_delta * (static_cast<Float64>(count) / static_cast<Float64>(total_count));
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
        return co_moment / static_cast<Float64>(count - 1);
    }

    static Float64 getCovarPop(Float64 co_moment, UInt64 count)
    {
        if (count == 0)
            return std::numeric_limits<Float64>::infinity();
        if (count == 1)
            return 0.0;
        return co_moment / static_cast<Float64>(count);
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
    /// varSampStable documentation
    FunctionDocumentation::Description description_varSampStable = R"(
Calculate the sample variance of a data set. Unlike [`varSamp`](/sql-reference/aggregate-functions/reference/varSamp), this function uses a [numerically stable](https://en.wikipedia.org/wiki/Numerical_stability) algorithm. It works slower but provides a lower computational error.

The sample variance is calculated using the same formula as [`varSamp`](/sql-reference/aggregate-functions/reference/varSamp):

$$
\frac{\Sigma{(x - \bar{x})^2}}{n-1}
$$

<br/>

Where:
- $x$ is each individual data point in the data set
- $\bar{x}$ is the arithmetic mean of the data set
- $n$ is the number of data points in the data set
    )";
    FunctionDocumentation::Syntax syntax_varSampStable = R"(
varSampStable(x)
    )";
    FunctionDocumentation::Arguments arguments_varSampStable = {
        {"x", "The population for which you want to calculate the sample variance.", {"(U)Int*", "Float*", "Decimal*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_varSampStable = {"Returns the sample variance of the input data set.", {"Float64"}};
    FunctionDocumentation::Examples examples_varSampStable = {
    {
        "Computing stable sample variance",
        R"(
DROP TABLE IF EXISTS test_data;
CREATE TABLE test_data
(
    x Float64
)
ENGINE = Memory;

INSERT INTO test_data VALUES (10.5), (12.3), (9.8), (11.2), (10.7);

SELECT round(varSampStable(x),3) AS var_samp_stable FROM test_data;
        )",
        R"(
┌─var_samp_stable─┐
│           0.865 │
└─────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_varSampStable = {1, 1};
    FunctionDocumentation::Category category_varSampStable = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_varSampStable = {description_varSampStable, syntax_varSampStable, arguments_varSampStable, {}, returned_value_varSampStable, examples_varSampStable, introduced_in_varSampStable, category_varSampStable};

    factory.registerFunction("varSampStable",
    {
        [](const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            assertNoParameters(name, parameters);
            assertUnary(name, argument_types);
            return std::make_shared<AggregateFunctionVariance>(VarKind::varSampStable, argument_types[0]);
        },
        documentation_varSampStable
    });

    /// varPopStable documentation
    FunctionDocumentation::Description description_varPopStable = R"(
Returns the population variance.
Unlike [`varPop`](/sql-reference/aggregate-functions/reference/varPop), this function uses a [numerically stable](https://en.wikipedia.org/wiki/Numerical_stability) algorithm.
It works slower but provides a lower computational error.
    )";
    FunctionDocumentation::Syntax syntax_varPopStable = R"(
varPopStable(x)
    )";
    FunctionDocumentation::Arguments arguments_varPopStable = {
        {"x", "Population of values to find the population variance of.", {"(U)Int*", "Float*", "Decimal*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_varPopStable = {"Returns the population variance of `x`.", {"Float64"}};
    FunctionDocumentation::Examples examples_varPopStable = {
    {
        "Computing stable population variance",
        R"(
DROP TABLE IF EXISTS test_data;
CREATE TABLE test_data
(
    x UInt8,
)
ENGINE = Memory;

INSERT INTO test_data VALUES (3),(3),(3),(4),(4),(5),(5),(7),(11),(15);

SELECT
    varPopStable(x) AS var_pop_stable
FROM test_data;
        )",
        R"(
┌─var_pop_stable─┐
│           14.4 │
└────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_varPopStable = {1, 1};
    FunctionDocumentation::Category category_varPopStable = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_varPopStable = {description_varPopStable, syntax_varPopStable, arguments_varPopStable, {}, returned_value_varPopStable, examples_varPopStable, introduced_in_varPopStable, category_varPopStable};

    factory.registerFunction("varPopStable",
    {
        [](const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            assertNoParameters(name, parameters);
            assertUnary(name, argument_types);
            return std::make_shared<AggregateFunctionVariance>(VarKind::varPopStable, argument_types[0]);
        },
        documentation_varPopStable
    });

    FunctionDocumentation::Description description_stddevSampStable = R"(
The result is equal to the square root of [varSamp](../../../sql-reference/aggregate-functions/reference/varSamp.md). Unlike [stddevSamp](../reference/stddevSamp.md) this function uses a numerically stable algorithm. It works slower but provides a lower computational error.
    )";
    FunctionDocumentation::Syntax syntax_stddevSampStable = R"(
stddevSampStable(x)
    )";
    FunctionDocumentation::Parameters parameters_stddevSampStable = {};
    FunctionDocumentation::Arguments arguments_stddevSampStable = {
        {"x", "Values for which to find the square root of sample variance.", {"(U)Int*", "Float*", "Decimal*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_stddevSampStable = {"Returns the square root of sample variance of `x`.", {"Float64"}};
    FunctionDocumentation::Examples examples_stddevSampStable = {
    {
        "Basic usage",
        R"(
DROP TABLE IF EXISTS test_data;
CREATE TABLE test_data
(
    population UInt8,
)
ENGINE = Log;

INSERT INTO test_data VALUES (3),(3),(3),(4),(4),(5),(5),(7),(11),(15);

SELECT
    stddevSampStable(population)
FROM test_data;
        )",
        R"(
┌─stddevSampStable(population)─┐
│                            4 │
└──────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_stddevSampStable = {1, 1};
    FunctionDocumentation::Category category_stddevSampStable = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_stddevSampStable = {description_stddevSampStable, syntax_stddevSampStable, arguments_stddevSampStable, parameters_stddevSampStable, returned_value_stddevSampStable, examples_stddevSampStable, introduced_in_stddevSampStable, category_stddevSampStable};

    factory.registerFunction("stddevSampStable", {[](const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
    {
        assertNoParameters(name, parameters);
        assertUnary(name, argument_types);
        return std::make_shared<AggregateFunctionVariance>(VarKind::stddevSampStable, argument_types[0]);
    }, documentation_stddevSampStable});

    FunctionDocumentation::Description description_stddevPopStable = R"(
The result is equal to the square root of [varPop](../../../sql-reference/aggregate-functions/reference/varPop.md). Unlike [stddevPop](../reference/stddevPop.md), this function uses a numerically stable algorithm. It works slower but provides a lower computational error.
    )";
    FunctionDocumentation::Syntax syntax_stddevPopStable = R"(
stddevPopStable(x)
    )";
    FunctionDocumentation::Parameters parameters_stddevPopStable = {};
    FunctionDocumentation::Arguments arguments_stddevPopStable = {
        {"x", "Population of values to find the standard deviation of.", {"(U)Int*", "Float*", "Decimal*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_stddevPopStable = {"Returns the square root of the variance of `x`.", {"Float64"}};
    FunctionDocumentation::Examples examples_stddevPopStable = {
    {
        "Basic usage",
        R"(
DROP TABLE IF EXISTS test_data;
CREATE TABLE test_data
(
    population Float64,
)
ENGINE = Log;

INSERT INTO test_data SELECT randUniform(5.5, 10) FROM numbers(1000000);

SELECT
    stddevPopStable(population) AS stddev
FROM test_data;
        )",
        R"(
┌─────────────stddev─┐
│ 1.2999977786592576 │
└────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_stddevPopStable = {1, 1};
    FunctionDocumentation::Category category_stddevPopStable = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_stddevPopStable = {description_stddevPopStable, syntax_stddevPopStable, arguments_stddevPopStable, parameters_stddevPopStable, returned_value_stddevPopStable, examples_stddevPopStable, introduced_in_stddevPopStable, category_stddevPopStable};

    factory.registerFunction("stddevPopStable", {[](const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
    {
        assertNoParameters(name, parameters);
        assertUnary(name, argument_types);
        return std::make_shared<AggregateFunctionVariance>(VarKind::stddevPopStable, argument_types[0]);
    }, documentation_stddevPopStable});

    FunctionDocumentation::Description covarSampStable_description = R"(
Calculates the sample covariance:

$$
\frac{\Sigma{(x - \bar{x})(y - \bar{y})}}{n - 1}
$$

<br/>

It is similar to [`covarSamp`](/sql-reference/aggregate-functions/reference/covarsamp) but uses a numerically stable algorithm.
As a result, `covarSampStable` is slower than `covarSamp` but provides a lower computational error.
    )";
    FunctionDocumentation::Syntax covarSampStable_syntax = "covarSampStable(x, y)";
    FunctionDocumentation::Arguments covarSampStable_arguments = {
        {"x", "First variable.", {"(U)Int*", "Float*", "Decimal"}},
        {"y", "Second variable.", {"(U)Int*", "Float*", "Decimal"}}
    };
    FunctionDocumentation::Parameters covarSampStable_parameters = {};
    FunctionDocumentation::ReturnedValue covarSampStable_returned_value = {"Returns the sample covariance between `x` and `y`. For `n <= 1`, `inf` is returned.", {"Float64"}};
    FunctionDocumentation::Examples covarSampStable_examples = {
    {
        "Basic sample covariance calculation with stable algorithm",
        R"(
DROP TABLE IF EXISTS series;
CREATE TABLE series(i UInt32, x_value Float64, y_value Float64) ENGINE = Memory;
INSERT INTO series(i, x_value, y_value) VALUES (1, 5.6,-4.4),(2, -9.6,3),(3, -1.3,-4),(4, 5.3,9.7),(5, 4.4,0.037),(6, -8.6,-7.8),(7, 5.1,9.3),(8, 7.9,-3.6),(9, -8.2,0.62),(10, -3,7.3);

SELECT covarSampStable(x_value, y_value)
FROM
(
    SELECT
        x_value,
        y_value
    FROM series
);
        )",
        R"(
┌─covarSampStable(x_value, y_value)─┐
│                 7.206275555555556 │
└───────────────────────────────────┘
        )"
    },
    {
        "Single value returns inf",
        R"(
SELECT covarSampStable(x_value, y_value)
FROM
(
    SELECT
        x_value,
        y_value
    FROM series LIMIT 1
);
        )",
        R"(
┌─covarSampStable(x_value, y_value)─┐
│                               inf │
└───────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::Category covarSampStable_category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation::IntroducedIn covarSampStable_introduced_in = {1, 1};
    FunctionDocumentation covarSampStable_documentation = {covarSampStable_description, covarSampStable_syntax, covarSampStable_arguments, covarSampStable_parameters, covarSampStable_returned_value, covarSampStable_examples, covarSampStable_introduced_in, covarSampStable_category};
    factory.registerFunction("covarSampStable",
    {
        [](const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            assertNoParameters(name, parameters);
            assertBinary(name, argument_types);
            return std::make_shared<AggregateFunctionCovariance<false>>(CovarKind::covarSampStable, argument_types);
        },
        covarSampStable_documentation
    });

    FunctionDocumentation::Description covarPopStable_description = R"(
Calculates the population covariance:

$$
\frac{\Sigma{(x - \bar{x})(y - \bar{y})}}{n}
$$

<br/>

It is similar to the [`covarPop`](/sql-reference/aggregate-functions/reference/covarpop) function, but uses a numerically stable algorithm. As a result, `covarPopStable` is slower than `covarPop` but produces a more accurate result.
    )";
    FunctionDocumentation::Syntax covarPopStable_syntax = "covarPopStable(x, y)";
    FunctionDocumentation::Arguments covarPopStable_arguments = {
        {"x", "First variable.", {"(U)Int*", "Float*", "Decimal"}},
        {"y", "Second variable.", {"(U)Int*", "Float*", "Decimal"}}
    };
    FunctionDocumentation::Parameters covarPopStable_parameters = {};
    FunctionDocumentation::ReturnedValue covarPopStable_returned_value = {"Returns the population covariance between `x` and `y`.", {"Float64"}};
    FunctionDocumentation::Examples covarPopStable_examples = {
    {
        "Basic population covariance calculation with stable algorithm",
        R"(
DROP TABLE IF EXISTS series;
CREATE TABLE series(i UInt32, x_value Float64, y_value Float64) ENGINE = Memory;
INSERT INTO series(i, x_value, y_value) VALUES (1, 5.6,-4.4),(2, -9.6,3),(3, -1.3,-4),(4, 5.3,9.7),(5, 4.4,0.037),(6, -8.6,-7.8),(7, 5.1,9.3),(8, 7.9,-3.6),(9, -8.2,0.62),(10, -3,7.3);

SELECT covarPopStable(x_value, y_value)
FROM series
        )",
        R"(
┌─covarPopStable(x_value, y_value)─┐
│                         6.485648 │
└──────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::Category covarPopStable_category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation::IntroducedIn covarPopStable_introduced_in = {1, 1};
    FunctionDocumentation covarPopStable_documentation = {covarPopStable_description, covarPopStable_syntax, covarPopStable_arguments, covarPopStable_parameters, covarPopStable_returned_value, covarPopStable_examples, covarPopStable_introduced_in, covarPopStable_category};
    factory.registerFunction("covarPopStable",
    {
        [](const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            assertNoParameters(name, parameters);
            assertBinary(name, argument_types);
            return std::make_shared<AggregateFunctionCovariance<false>>(CovarKind::covarPopStable, argument_types);
        },
        covarPopStable_documentation
    });

    FunctionDocumentation::Description corrStable_description = R"(
Calculates the [Pearson correlation coefficient](https://en.wikipedia.org/wiki/Pearson_correlation_coefficient):

$$
\frac{\Sigma{(x - \bar{x})(y - \bar{y})}}{\sqrt{\Sigma{(x - \bar{x})^2} * \Sigma{(y - \bar{y})^2}}}
$$

<br/>

Similar to the [`corr`](../reference/corr.md) function, but uses a numerically stable algorithm.
As a result, `corrStable` is slower than `corr` but produces a more accurate result.
    )";
    FunctionDocumentation::Syntax corrStable_syntax = "corrStable(x, y)";
    FunctionDocumentation::Arguments corrStable_arguments = {
        {"x", "First variable.", {"(U)Int*", "Float*", "Decimal"}},
        {"y", "Second variable.", {"(U)Int*", "Float*", "Decimal"}}
    };
    FunctionDocumentation::Parameters corrStable_parameters = {};
    FunctionDocumentation::ReturnedValue corrStable_returned_value = {"Returns the Pearson correlation coefficient.", {"Float64"}};
    FunctionDocumentation::Examples corrStable_examples = {
    {
        "Basic correlation calculation with stable algorithm",
        R"(
DROP TABLE IF EXISTS series;
CREATE TABLE series
(
    i UInt32,
    x_value Float64,
    y_value Float64
)
ENGINE = Memory;
INSERT INTO series(i, x_value, y_value) VALUES (1, 5.6, -4.4),(2, -9.6, 3),(3, -1.3, -4),(4, 5.3, 9.7),(5, 4.4, 0.037),(6, -8.6, -7.8),(7, 5.1, 9.3),(8, 7.9, -3.6),(9, -8.2, 0.62),(10, -3, 7.3);

SELECT corrStable(x_value, y_value)
FROM series
        )",
        R"(
┌─corrStable(x_value, y_value)─┐
│          0.17302657554532558 │
└──────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::Category corrStable_category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation::IntroducedIn corrStable_introduced_in = {1, 1};
    FunctionDocumentation corrStable_documentation = {corrStable_description, corrStable_syntax, corrStable_arguments, corrStable_parameters, corrStable_returned_value, corrStable_examples, corrStable_introduced_in, corrStable_category};

    factory.registerFunction("corrStable",
    {
        [](const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            assertNoParameters(name, parameters);
            assertBinary(name, argument_types);
            return std::make_shared<AggregateFunctionCovariance<true>>(CovarKind::corrStable, argument_types);
        },
        corrStable_documentation
    });
}

}
