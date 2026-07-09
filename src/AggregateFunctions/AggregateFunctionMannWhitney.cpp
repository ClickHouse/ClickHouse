#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/StatCommon.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnTuple.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <limits>

#include <boost/math/distributions/normal.hpp>


namespace DB
{

struct Settings;

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
}

namespace
{

struct MannWhitneyData : public StatisticalSample<Float64, Float64>
{
    /*Since null hypothesis is "for randomly selected values X and Y from two populations,
     *the probability of X being greater than Y is equal to the probability of Y being greater than X".
     *Or "the distribution F of first sample equals to the distribution G of second sample".
     *Then alternative for this hypothesis (H1) is "two-sided"(F != G), "less"(F < G), "greater" (F > G). */
    enum class Alternative : uint8_t
    {
        TwoSided,
        Less,
        Greater
    };

    /// The behaviour equals to the similar function from scipy.
    /// https://github.com/scipy/scipy/blob/ab9e9f17e0b7b2d618c4d4d8402cd4c0c200d6c0/scipy/stats/stats.py#L6978
    std::pair<Float64, Float64> getResult(Alternative alternative, bool continuity_correction)
    {
        ConcatenatedSamples both(this->x, this->y);
        RanksArray ranks;
        Float64 tie_correction;

        /// Compute ranks according to both samples.
        std::tie(ranks, tie_correction) = computeRanksAndTieCorrection(both);

        const Float64 n1 = static_cast<Float64>(this->size_x);
        const Float64 n2 = static_cast<Float64>(this->size_y);

        Float64 r1 = 0;
        for (size_t i = 0; i < static_cast<size_t>(n1); ++i)
            r1 += ranks[i];

        const Float64 u1 = n1 * n2 + (n1 * (n1 + 1.)) / 2. - r1;
        const Float64 u2 = n1 * n2 - u1;

        /// The distribution of U-statistic under null hypothesis H0  is symmetric with respect to meanrank.
        const Float64 meanrank = n1 * n2 /2. + 0.5 * continuity_correction;

        /// Handle the case when tie_correction is close to zero (all values are identical)
        Float64 sd = 0.0;
        if (std::abs(tie_correction) > std::numeric_limits<Float64>::epsilon())
            sd = std::sqrt(tie_correction * n1 * n2 * (n1 + n2 + 1) / 12.0);

        Float64 u = 0;
        if (alternative == Alternative::TwoSided)
            /// There is no difference which u_i to take as u, because z will be differ only in sign and we take std::abs() from it.
            u = std::max(u1, u2);
        else if (alternative == Alternative::Less)
            u = u1;
        else if (alternative == Alternative::Greater)
            u = u2;

        /// If the standard deviation is close to zero (all values are identical),
        /// z will be 0, which leads to p-value = 0.5 for one-sided tests
        /// and p-value = 1.0 for two-sided tests
        Float64 z = 0.0;
        if (sd > std::numeric_limits<Float64>::epsilon())
            z = (u - meanrank) / sd;

        if (unlikely(!std::isfinite(z)))
            return {std::numeric_limits<Float64>::quiet_NaN(), std::numeric_limits<Float64>::quiet_NaN()};

        if (alternative == Alternative::TwoSided)
            z = std::abs(z);

        auto standard_normal_distribution = boost::math::normal_distribution<Float64>();
        auto cdf = boost::math::cdf(standard_normal_distribution, z);

        Float64 p_value = 0;
        if (alternative == Alternative::TwoSided)
            p_value = 2 - 2 * cdf;
        else
            p_value = 1 - cdf;

        return {u2, p_value};
    }

private:
    using Sample = typename StatisticalSample<Float64, Float64>::SampleX;

    /// We need to compute ranks according to all samples. Use this class to avoid extra copy and memory allocation.
    class ConcatenatedSamples
    {
        public:
            ConcatenatedSamples(const Sample & first_, const Sample & second_)
                : first(first_), second(second_) {}

            const Float64 & operator[](size_t ind) const
            {
                if (ind < first.size())
                    return first[ind];
                return second[ind - first.size()];
            }

            size_t size() const
            {
                return first.size() + second.size();
            }

        private:
            const Sample & first;
            const Sample & second;
    };
};

class AggregateFunctionMannWhitney final:
    public IAggregateFunctionDataHelper<MannWhitneyData, AggregateFunctionMannWhitney>
{
private:
    using Alternative = typename MannWhitneyData::Alternative;
    Alternative alternative;
    bool continuity_correction{true};

public:
    explicit AggregateFunctionMannWhitney(const DataTypes & arguments, const Array & params)
        : IAggregateFunctionDataHelper<MannWhitneyData, AggregateFunctionMannWhitney> ({arguments}, {}, createResultType())
    {
        if (params.size() > 2)
            throw Exception(ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION, "Aggregate function {} require two parameter or less", getName());

        if (params.empty())
        {
            alternative = Alternative::TwoSided;
            return;
        }

        if (params[0].getType() != Field::Types::String)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Aggregate function {} require first parameter to be a String", getName());

        const auto & param = params[0].safeGet<String>();
        if (param == "two-sided")
            alternative = Alternative::TwoSided;
        else if (param == "less")
            alternative = Alternative::Less;
        else if (param == "greater")
            alternative = Alternative::Greater;
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown parameter in aggregate function {}. "
                    "It must be one of: 'two-sided', 'less', 'greater'", getName());

        if (params.size() != 2)
            return;

        if (params[1].getType() != Field::Types::UInt64)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Aggregate function {} require second parameter to be a UInt64", getName());

        continuity_correction = static_cast<bool>(params[1].safeGet<UInt64>());
    }

    String getName() const override
    {
        return "mannWhitneyUTest";
    }

    bool allocatesMemoryInArena() const override { return true; }

    static DataTypePtr createResultType()
    {
        DataTypes types
        {
            std::make_shared<DataTypeNumber<Float64>>(),
            std::make_shared<DataTypeNumber<Float64>>(),
        };

        Strings names
        {
            "u_statistic",
            "p_value"
        };

        return std::make_shared<DataTypeTuple>(
            std::move(types),
            std::move(names)
        );
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        Float64 value = columns[0]->getFloat64(row_num);
        bool is_second = columns[1]->getUInt(row_num);

        if (is_second)
            data(place).addY(value, arena);
        else
            data(place).addX(value, arena);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        auto & a = data(place);
        const auto & b = data(rhs);

        a.merge(b, arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        data(place).read(buf, arena);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        if (!data(place).size_x || !data(place).size_y)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Aggregate function {} require both samples to be non empty", getName());

        auto [u_statistic, p_value] = data(place).getResult(alternative, continuity_correction);

        /// Because p-value is a probability.
        p_value = std::min(1.0, std::max(0.0, p_value));

        auto & column_tuple = assert_cast<ColumnTuple &>(to);
        auto & column_stat = assert_cast<ColumnVector<Float64> &>(column_tuple.getColumn(0));
        auto & column_value = assert_cast<ColumnVector<Float64> &>(column_tuple.getColumn(1));

        column_stat.getData().push_back(u_statistic);
        column_value.getData().push_back(p_value);
    }

};


AggregateFunctionPtr createAggregateFunctionMannWhitneyUTest(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertBinary(name, argument_types);

    if (!isNumber(argument_types[0]) || !isNumber(argument_types[1]))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Aggregate function {} only supports numerical types", name);

    return std::make_shared<AggregateFunctionMannWhitney>(argument_types, parameters);
}

}


void registerAggregateFunctionMannWhitney(AggregateFunctionFactory & factory)
{
    FunctionDocumentation::Description description = R"(
Applies the Mann-Whitney rank test to samples from two populations.

Values of both samples are in the `sample_data` column.
If `sample_index` equals to 0 then the value in that row belongs to the sample from the first population.
Otherwise it belongs to the sample from the second population.
The null hypothesis is that two populations are stochastically equal.
Also one-sided hypotheses can be tested.
This test does not assume that data have normal distribution.
    )";
    FunctionDocumentation::Syntax syntax = R"(
mannWhitneyUTest[(alternative[, continuity_correction])](sample_data, sample_index)
    )";
    FunctionDocumentation::Arguments arguments = {
        {"sample_data", "Sample data.", {"(U)Int*", "Float*", "Decimal*"}},
        {"sample_index", "Sample index.", {"(U)Int*"}}
    };
    FunctionDocumentation::Parameters parameters = {
        {"alternative", "Optional. Alternative hypothesis. 'two-sided' (default): two populations are not stochastically equal. 'greater': values in the first sample are stochastically greater than those in the second sample. 'less': values in the first sample are stochastically less than those in the second sample.", {"String"}},
        {"continuity_correction", "Optional. If not 0 then continuity correction in the normal approximation for the p-value is applied. The default value is 1.", {"UInt64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a tuple with two elements: calculated U-statistic and calculated p-value.", {"Tuple(Float64, Float64)"}};
    FunctionDocumentation::Examples examples = {
    {
        "Mann-Whitney U test example",
        R"(
CREATE TABLE mww_ttest (sample_data Float64, sample_index UInt8) ENGINE = Memory;
INSERT INTO mww_ttest VALUES (10, 0), (11, 0), (12, 0), (1, 1), (2, 1), (3, 1);

SELECT mannWhitneyUTest('greater')(sample_data, sample_index) FROM mww_ttest;
        )",
        R"(
┌─mannWhitneyUTest('greater')(sample_data, sample_index)─┐
│ (9,0.04042779918503192)                                │
└────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {21, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation = {description, syntax, arguments, parameters, returned_value, examples, introduced_in, category};

    factory.registerFunction("mannWhitneyUTest", {createAggregateFunctionMannWhitneyUTest, documentation, {}});
}

}
