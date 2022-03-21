#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/StatCommon.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnTuple.h>
#include <Common/assert_cast.h>
#include <Core/Types.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <cmath>
#include <cfloat>


/// This function is used in implementations of different T-Tests.
/// On Darwin it's unavailable in math.h but actually exists in the library (can be linked successfully).
#if defined(OS_DARWIN)
extern "C"
{
    double lgamma_r(double x, int * signgamp);
}
#endif


namespace DB
{
struct Settings;

class ReadBuffer;
class WriteBuffer;

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

/**
 * If you have a cumulative distribution function F, then calculating the p-value for given statistic T is simply 1−F(T)
 * In our case p-value is two-sided, so we multiply it by 2.
 * So cumulative distribution function F equals to
 * \[ F(t) = \int_{-\infty}^{t} f(u)du = 1 - \frac{1}{2} I_{x(t)}(\frac{v}{2}, \frac{1}{2}) \]
 * where \[ x(t) = \frac{v}{t^2 + v} \]: https://en.wikipedia.org/wiki/Student%27s_t-distribution#Cumulative_distribution_function
 *
 * so our resulting \[ p-value = I_{x(t)}(\frac{v}{2}, \frac{1}{2}) \].
 *
 * And I is regularized incomplete beta function: https://en.wikipedia.org/wiki/Beta_function#Incomplete_beta_function
 *
 * Keepenig in mind that \[ \mathrm {B} (x;a,b)=\int _{0}^{x}r^{a-1}\,(1-r)^{b-1}\,\mathrm {d} r.\! \]
 * and
 * \[ \mathrm {B} (x,y)={\dfrac {\Gamma (x)\,\Gamma (y)}{\Gamma (x+y)}}=\
 * \exp(\ln {\dfrac {\Gamma (x)\,\Gamma (y)}{\Gamma (x+y)}})=\exp((\ln(\Gamma (x))+\ln(\Gamma (y))-\ln(\Gamma (x+y))) \]
 *
 * p-value can be calculated in terms of gamma functions and integrals more simply:
 * \[ {\frac {\int _{0}^{\frac {\nu }{t^{2}+\nu }}r^{{\frac {\nu }{2}}-1}\,(1-r)^{-0.5}\,\mathrm {d} r}\
 * {\exp((\ln(\Gamma ({\frac {\nu }{2}}))+\ln(\Gamma (0.5))-\ln(\Gamma ({\frac {\nu }{2}}+0.5)))}} \]
 *
 * which simplifies to:
 *
 * \[ {\frac {\int _{0}^{\frac {\nu }{t^{2}+\nu }}{\frac {r^{{\frac {\nu }{2}}-1}}{\sqrt {1-r}}}\,\mathrm {d} r}\
 * {\exp((\ln(\Gamma ({\frac {\nu }{2}}))+\ln(\Gamma (0.5))-\ln(\Gamma ({\frac {\nu }{2}}+0.5)))}} \]
 *
 * Read here for details https://rosettacode.org/wiki/Welch%27s_t-test#
 *
 * Both WelchTTest and StudentTTest have t-statistric with Student distribution but with different degrees of freedom.
 * So the procedure of computing p-value is the same.
*/
static inline Float64 getPValue(Float64 degrees_of_freedom, Float64 t_stat2) /// NOLINT
{
    Float64 numerator = integrateSimpson(0, degrees_of_freedom / (t_stat2 + degrees_of_freedom),
        [degrees_of_freedom](double x) { return std::pow(x, degrees_of_freedom / 2 - 1) / std::sqrt(1 - x); });

    int unused;
    Float64 denominator = std::exp(
        lgamma_r(degrees_of_freedom / 2, &unused)
        + lgamma_r(0.5, &unused)
        - lgamma_r(degrees_of_freedom / 2 + 0.5, &unused));

    return std::min(1.0, std::max(0.0, numerator / denominator));
}


/// Returns tuple of (t-statistic, p-value)
/// https://cpb-us-w2.wpmucdn.com/voices.uchicago.edu/dist/9/1193/files/2016/01/05b-TandP.pdf
template <typename Data>
class AggregateFunctionTTest :
    public IAggregateFunctionDataHelper<Data, AggregateFunctionTTest<Data>>
{
private:
    bool need_confidence_interval = false;
    Float64 confidence_level;
public:
    AggregateFunctionTTest(const DataTypes & arguments, const Array & params)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionTTest<Data>>({arguments}, params)
    {
        if (!params.empty())
        {
            need_confidence_interval = true;
            confidence_level = params.at(0).safeGet<Float64>();

            if (!std::isfinite(confidence_level))
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Aggregate function {} requires finite parameter values.", Data::name);
            }

            if (confidence_level <= 0.0 || confidence_level >= 1.0 || fabs(confidence_level - 0.0) < DBL_EPSILON || fabs(confidence_level - 1.0) < DBL_EPSILON)
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Confidence level parameter must be between 0 and 1 in aggregate function {}.", Data::name);
            }

        }
    }

    String getName() const override
    {
        return Data::name;
    }

    DataTypePtr getReturnType() const override
    {
        if (need_confidence_interval)
        {
            DataTypes types
            {
                std::make_shared<DataTypeNumber<Float64>>(),
                std::make_shared<DataTypeNumber<Float64>>(),
                std::make_shared<DataTypeNumber<Float64>>(),
                std::make_shared<DataTypeNumber<Float64>>(),
            };

            Strings names
            {
                "t_statistic",
                "p_value",
                "confidence_interval_low",
                "confidence_interval_high",
            };

            return std::make_shared<DataTypeTuple>(
                std::move(types),
                std::move(names)
            );
        }
        else
        {
            DataTypes types
            {
                std::make_shared<DataTypeNumber<Float64>>(),
                std::make_shared<DataTypeNumber<Float64>>(),
            };

            Strings names
            {
                "t_statistic",
                "p_value",
            };

            return std::make_shared<DataTypeTuple>(
                std::move(types),
                std::move(names)
            );
        }
    }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        Float64 value = columns[0]->getFloat64(row_num);
        UInt8 is_second = columns[1]->getUInt(row_num);

        if (is_second)
            this->data(place).addY(value);
        else
            this->data(place).addX(value);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version  */, Arena *) const override
    {
        this->data(place).read(buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto & data = this->data(place);
        auto & column_tuple = assert_cast<ColumnTuple &>(to);

        if (!data.hasEnoughObservations() || data.isEssentiallyConstant())
        {
            auto & column_stat = assert_cast<ColumnVector<Float64> &>(column_tuple.getColumn(0));
            auto & column_value = assert_cast<ColumnVector<Float64> &>(column_tuple.getColumn(1));
            column_stat.getData().push_back(std::numeric_limits<Float64>::quiet_NaN());
            column_value.getData().push_back(std::numeric_limits<Float64>::quiet_NaN());

            if (need_confidence_interval)
            {
                auto & column_ci_low = assert_cast<ColumnVector<Float64> &>(column_tuple.getColumn(2));
                auto & column_ci_high = assert_cast<ColumnVector<Float64> &>(column_tuple.getColumn(3));
                column_ci_low.getData().push_back(std::numeric_limits<Float64>::quiet_NaN());
                column_ci_high.getData().push_back(std::numeric_limits<Float64>::quiet_NaN());
            }

            return;
        }

        auto [t_statistic, p_value] = data.getResult();

        /// Because p-value is a probability.
        p_value = std::min(1.0, std::max(0.0, p_value));

        auto & column_stat = assert_cast<ColumnVector<Float64> &>(column_tuple.getColumn(0));
        auto & column_value = assert_cast<ColumnVector<Float64> &>(column_tuple.getColumn(1));

        column_stat.getData().push_back(t_statistic);
        column_value.getData().push_back(p_value);

        if (need_confidence_interval)
        {
            auto [ci_low, ci_high] = data.getConfidenceIntervals(confidence_level, data.getDegreesOfFreedom());
            auto & column_ci_low = assert_cast<ColumnVector<Float64> &>(column_tuple.getColumn(2));
            auto & column_ci_high = assert_cast<ColumnVector<Float64> &>(column_tuple.getColumn(3));
            column_ci_low.getData().push_back(ci_low);
            column_ci_high.getData().push_back(ci_high);
        }
    }
};

};
