#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/StatCommon.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnTuple.h>
#include <Common/assert_cast.h>
#include <Common/PODArray_fwd.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <cmath>
#include <limits>


namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}
}

namespace DB
{
struct Settings;

namespace
{

struct RankCorrelationData : public StatisticalSample<Float64, Float64>
{
    /// add() stores both coordinates of a row or neither, so unequal sample sizes mean a state
    /// written without row pairing, whose surviving pairs are unrecoverable. merge() concatenates
    /// the vectors and can cancel the skew, so the verdict is taken where the skew is visible.
    bool pairing_lost = false;

    Float64 getResult()
    {
        const size_t size = this->size_x;

        if (pairing_lost || size != this->size_y || size < 2)
            return std::numeric_limits<Float64>::quiet_NaN();

        const RanksArray ranks_x = computeRanksAndTieCorrection(this->x).first;
        const RanksArray ranks_y = computeRanksAndTieCorrection(this->y).first;

        /// Spearman's coefficient is the Pearson correlation of the mid-ranks. The
        /// closed-form 1 - 6 * sum(d^2) / (n^3 - n) is only valid without ties, because
        /// ties shrink the rank variance below the value that form assumes.
        Float64 mean_x = 0;
        Float64 mean_y = 0;
        for (size_t j = 0; j < size; ++j)
        {
            mean_x += ranks_x[j];
            mean_y += ranks_y[j];
        }
        mean_x /= static_cast<Float64>(size);
        mean_y /= static_cast<Float64>(size);

        Float64 covariance = 0;
        Float64 deviation_x = 0;
        Float64 deviation_y = 0;
        for (size_t j = 0; j < size; ++j)
        {
            const Float64 dx = ranks_x[j] - mean_x;
            const Float64 dy = ranks_y[j] - mean_y;
            covariance += dx * dy;
            deviation_x += dx * dx;
            deviation_y += dy * dy;
        }

        /// A constant column has no rank variance, so the correlation is undefined.
        const Float64 denominator = deviation_x * deviation_y;
        if (denominator == 0)
            return std::numeric_limits<Float64>::quiet_NaN();

        /// Multiply before taking the root: sqrt(dx) * sqrt(dy) can round the perfectly
        /// correlated case to 1.0000000000000002, outside the documented [-1, +1] range.
        return covariance / std::sqrt(denominator);
    }
};

class AggregateFunctionRankCorrelation final :
    public IAggregateFunctionDataHelper<RankCorrelationData, AggregateFunctionRankCorrelation>
{
public:
    explicit AggregateFunctionRankCorrelation(const DataTypes & arguments)
        : IAggregateFunctionDataHelper<RankCorrelationData, AggregateFunctionRankCorrelation> ({arguments}, {}, std::make_shared<DataTypeNumber<Float64>>())
    {}

    String getName() const override
    {
        return "rankCorr";
    }

    bool allocatesMemoryInArena() const override { return true; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        Float64 new_x = columns[0]->getFloat64(row_num);
        Float64 new_y = columns[1]->getFloat64(row_num);

        /// Keep observations paired: StatisticalSample skips NaNs per column, which would
        /// otherwise correlate ranks coming from different rows.
        if (isNaN(new_x) || isNaN(new_y))
            return;

        data(place).addX(new_x, arena);
        data(place).addY(new_y, arena);
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        auto & a = data(place);
        const auto & b = data(rhs);

        a.merge(b, arena);
        a.pairing_lost |= b.pairing_lost;
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        auto & sample = data(place);
        sample.read(buf, arena);
        sample.pairing_lost |= sample.size_x != sample.size_y;
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto answer = data(place).getResult();

        auto & column = static_cast<ColumnVector<Float64> &>(to);
        column.getData().push_back(answer);
    }

};


AggregateFunctionPtr createAggregateFunctionRankCorrelation(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertBinary(name, argument_types);
    assertNoParameters(name, parameters);

    if (!isNumber(argument_types[0]) || !isNumber(argument_types[1]))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Aggregate function {} only supports numerical types", name);

    return std::make_shared<AggregateFunctionRankCorrelation>(argument_types);
}

}


void registerAggregateFunctionRankCorrelation(AggregateFunctionFactory & factory);
void registerAggregateFunctionRankCorrelation(AggregateFunctionFactory & factory)
{
    FunctionDocumentation::Description description_rankCorr = R"(
Computes a rank correlation coefficient.

Returns a rank correlation coefficient of the ranks of x and y. The value of the correlation coefficient ranges from -1 to +1. If less than two arguments are passed, the function will return an exception. The value close to +1 denotes a high linear relationship, and with an increase of one random variable, the second random variable also increases. The value close to -1 denotes a high linear relationship, and with an increase of one random variable, the second random variable decreases. The value close or equal to 0 denotes no relationship between the two random variables. Rows where either argument is `nan` are skipped. Returns `nan` when the correlation is undefined: fewer than two remaining rows, or all values in either argument equal.

**See Also**

- [Spearman's rank correlation coefficient](https://en.wikipedia.org/wiki/Spearman%27s_rank_correlation_coefficient)
    )";
    FunctionDocumentation::Syntax syntax_rankCorr = R"(
rankCorr(x, y)
    )";
    FunctionDocumentation::Parameters parameters_rankCorr = {};
    FunctionDocumentation::Arguments arguments_rankCorr = {
        {"x", "Arbitrary value.", {"Float*"}},
        {"y", "Arbitrary value.", {"Float*"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_rankCorr = {"Returns a rank correlation coefficient of the ranks of x and y. The value ranges from -1 to +1, or `nan` when the correlation is undefined.", {"Float64"}};
    FunctionDocumentation::Examples examples_rankCorr = {
    {
        "Perfect correlation",
        R"(
SELECT rankCorr(number, number) FROM numbers(100);
        )",
        R"(
┌─rankCorr(number, number)─┐
│                        1 │
└──────────────────────────┘
        )"
    },
    {
        "Non-linear relationship",
        R"(
SELECT roundBankers(rankCorr(exp(number), sin(number)), 3) FROM numbers(100);
        )",
        R"(
┌─roundBankers(rankCorr(exp(number), sin(number)), 3)─┐
│                                              -0.037 │
└─────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_rankCorr = {20, 9};
    FunctionDocumentation::Category category_rankCorr = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation_rankCorr = {description_rankCorr, syntax_rankCorr, arguments_rankCorr, parameters_rankCorr, returned_value_rankCorr, examples_rankCorr, introduced_in_rankCorr, category_rankCorr};

    factory.registerFunction("rankCorr", {createAggregateFunctionRankCorrelation, documentation_rankCorr});
}

}
