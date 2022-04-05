#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/StatCommon.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnTuple.h>
#include <Common/assert_cast.h>
#include <Common/PODArray_fwd.h>
#include <base/types.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <limits>

#include <DataTypes/DataTypeArray.h>

#include <Common/ArenaAllocator.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}


struct MannWhitneyData : public StatisticalSample<Float64, Float64>
{
    /*Since null hypothesis is "for randomly selected values X and Y from two populations,
     *the probability of X being greater than Y is equal to the probability of Y being greater than X".
     *Or "the distribution F of first sample equals to the distribution G of second sample".
     *Then alternative for this hypothesis (H1) is "two-sided"(F != G), "less"(F < G), "greater" (F > G). */
    enum class Alternative
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

        const Float64 n1 = this->size_x;
        const Float64 n2 = this->size_y;

        Float64 r1 = 0;
        for (size_t i = 0; i < n1; ++i)
            r1 += ranks[i];

        const Float64 u1 = n1 * n2 + (n1 * (n1 + 1.)) / 2. - r1;
        const Float64 u2 = n1 * n2 - u1;

        /// The distribution of U-statistic under null hypothesis H0  is symmetric with respect to meanrank.
        const Float64 meanrank = n1 * n2 /2. + 0.5 * continuity_correction;
        const Float64 sd = std::sqrt(tie_correction * n1 * n2 * (n1 + n2 + 1) / 12.0);

        Float64 u = 0;
        if (alternative == Alternative::TwoSided)
            /// There is no difference which u_i to take as u, because z will be differ only in sign and we take std::abs() from it.
            u = std::max(u1, u2);
        else if (alternative == Alternative::Less)
            u = u1;
        else if (alternative == Alternative::Greater)
            u = u2;

        Float64 z = (u - meanrank) / sd;
        if (alternative == Alternative::TwoSided)
            z = std::abs(z);

        /// In fact cdf is a probability function, so it is intergral of density from (-inf, z].
        /// But since standard normal distribution is symmetric, cdf(0) = 0.5 and we have to compute integral from [0, z].
        const Float64 cdf = integrateSimpson(0, z, [] (Float64 t) { return std::pow(M_E, -0.5 * t * t) / std::sqrt(2 * M_PI);});

        Float64 p_value = 0;
        if (alternative == Alternative::TwoSided)
            p_value = 1 - 2 * cdf;
        else
            p_value = 0.5 - cdf;

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
                return second[ind % first.size()];
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
        :IAggregateFunctionDataHelper<MannWhitneyData, AggregateFunctionMannWhitney> ({arguments}, {})
    {
        if (params.size() > 2)
            throw Exception("Aggregate function " + getName() + " require two parameter or less", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (params.empty())
        {
            alternative = Alternative::TwoSided;
            return;
        }

        if (params[0].getType() != Field::Types::String)
            throw Exception("Aggregate function " + getName() + " require first parameter to be a String", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        const auto & param = params[0].get<String>();
        if (param == "two-sided")
            alternative = Alternative::TwoSided;
        else if (param == "less")
            alternative = Alternative::Less;
        else if (param == "greater")
            alternative = Alternative::Greater;
        else
            throw Exception("Unknown parameter in aggregate function " + getName() +
                    ". It must be one of: 'two-sided', 'less', 'greater'", ErrorCodes::BAD_ARGUMENTS);

        if (params.size() != 2)
            return;

        if (params[1].getType() != Field::Types::UInt64)
                throw Exception("Aggregate function " + getName() + " require second parameter to be a UInt64", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        continuity_correction = static_cast<bool>(params[1].get<UInt64>());
    }

    String getName() const override
    {
        return "mannWhitneyUTest";
    }

    bool allocatesMemoryInArena() const override { return true; }

    DataTypePtr getReturnType() const override
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
        UInt8 is_second = columns[1]->getUInt(row_num);

        if (is_second)
            this->data(place).addY(value, arena);
        else
            this->data(place).addX(value, arena);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        auto & a = this->data(place);
        const auto & b = this->data(rhs);

        a.merge(b, arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        this->data(place).read(buf, arena);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        if (!this->data(place).size_x || !this->data(place).size_y)
            throw Exception("Aggregate function " + getName() + " require both samples to be non empty", ErrorCodes::BAD_ARGUMENTS);

        auto [u_statistic, p_value] = this->data(place).getResult(alternative, continuity_correction);

        /// Because p-value is a probability.
        p_value = std::min(1.0, std::max(0.0, p_value));

        auto & column_tuple = assert_cast<ColumnTuple &>(to);
        auto & column_stat = assert_cast<ColumnVector<Float64> &>(column_tuple.getColumn(0));
        auto & column_value = assert_cast<ColumnVector<Float64> &>(column_tuple.getColumn(1));

        column_stat.getData().push_back(u_statistic);
        column_value.getData().push_back(p_value);
    }

};

};
