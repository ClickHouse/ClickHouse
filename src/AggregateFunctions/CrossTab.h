#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/UniqVariadicHash.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/HashTable/HashMap.h>
#include <Common/assert_cast.h>


/** Aggregate function that calculates statistics on top of cross-tab:
  * - histogram of every argument and every pair of elements.
  * These statistics include:
  * - Cramer's V;
  * - Theil's U;
  * - contingency coefficient;
  * It can be interpreted as interdependency coefficient between arguments;
  * or non-parametric correlation coefficient.
  */
namespace DB
{

struct CrossTabData
{
    /// Total count.
    UInt64 count = 0;

    /// Count of every value of the first and second argument (values are pre-hashed).
    /// Note: non-cryptographic 64bit hash is used, it means that the calculation is approximate.
    HashMapWithStackMemory<UInt64, UInt64, TrivialHash, 4> count_a;
    HashMapWithStackMemory<UInt64, UInt64, TrivialHash, 4> count_b;

    /// Count of every pair of values. We pack two hashes into UInt128.
    HashMapWithStackMemory<UInt128, UInt64, UInt128Hash, 4> count_ab;


    void add(UInt64 hash1, UInt64 hash2)
    {
        ++count;
        ++count_a[hash1];
        ++count_b[hash2];

        UInt128 hash_pair{hash1, hash2};
        ++count_ab[hash_pair];
    }

    void merge(const CrossTabData & other)
    {
        count += other.count;
        for (const auto & [key, value] : other.count_a)
            count_a[key] += value;
        for (const auto & [key, value] : other.count_b)
            count_b[key] += value;
        for (const auto & [key, value] : other.count_ab)
            count_ab[key] += value;
    }

    void serialize(WriteBuffer & buf) const
    {
        writeBinary(count, buf);
        count_a.write(buf);
        count_b.write(buf);
        count_ab.write(buf);
    }

    void deserialize(ReadBuffer & buf)
    {
        readBinary(count, buf);
        count_a.read(buf);
        count_b.read(buf);
        count_ab.read(buf);
    }

    /** See https://en.wikipedia.org/wiki/Cram%C3%A9r%27s_V
      *
      * φ² is χ² divided by the sample size (count).
      * χ² is the sum of squares of the normalized differences between the "expected" and "observed" statistics.
      * ("Expected" in the case when one of the hypotheses is true).
      * Something resembling the L2 distance.
      *
      * Note: statisticians use the name χ² for every statistic that has χ² distribution in many various contexts.
      *
      * Let's suppose that there is no association between the values a and b.
      * Then the frequency (e.g. probability) of (a, b) pair is equal to the multiplied frequencies of a and b:
      * count_ab / count = (count_a / count) * (count_b / count)
      * count_ab = count_a * count_b / count
      *
      * Let's calculate the difference between the values that are supposed to be equal if there is no association between a and b:
      * count_ab - count_a * count_b / count
      *
      * Let's sum the squares of the differences across all (a, b) pairs, including unobserved pairs (count_ab = 0):
      * Then divide by the second term for normalization: (count_a * count_b / count)
      * We get:
      *   χ² = Σ_{a,b} (count_ab - (count_a * count_b) / count)² / ((count_a * count_b) / count).
      *
      * However, iterating over all possible pairs (a, b) is not feasible: in the worst case it is quadratic, O(|A||B|)
      * if all entries may have count_ab = 0.
      * Instead, we can rewrite the formula into a sparse form that runs in O(N), where N is the number of
      * observed pairs (count_ab > 0), avoiding explicit iteration over zero cells.
      *
      * Expand (X - Y)² / Y = X² / Y - 2X + Y and sum term-wise:
      *   χ² = Σ_{a,b} count_ab² / ((count_a * count_b) / count)
      *        - 2 Σ_{a,b} count_ab
      *        + Σ_{a,b} (count_a * count_b) / count.
      *
      * Note that:
      *   Σ_{a,b} count_ab = count,
      *   Σ_{a,b} (count_a * count_b) / count
      *     = (1 / count) * Σ_a Σ_b (count_a * count_b)
      *     = (1 / count) * Σ_a [ count_a * Σ_b count_b ]  // count_a does not depend on b
      *     = (1 / count) * (Σ_a count_a) * (Σ_b count_b)
      *     = (1 / count) * count * count
      *     = count.
      *
      * Therefore
      *   χ² = Σ_{a,b} (count * count_ab²) / (count_a * count_b) - count.
      *
      * Divide by count to obtain φ² = χ² / count:
      *   φ² = Σ_{a,b} (count_ab²) / (count_a * count_b) - 1.
      *
      * Note that in this formulation we iterate only over observed pairs (count_ab > 0),
      * because terms with count_ab = 0 contribute 0 to the sum; thus the computation is linear
      * in the number of observed pairs (O(N), where N = |count_ab|).
      *
      * This will be the χ² statistic.
      * This statistic is used as a base for many other statistics.
      */
    Float64 getPhiSquared() const
    {
        if (count == 0)
        {
            return 0.0;
        }

        // We compute Σ_{a,b} (count_ab² ) / (count_a * count_b) part of the formula first
        Float64 sum = 0.0;
        for (const auto & [key, value_ab_uint] : count_ab)
        {
            const Float64 value_a = count_a.at(key.items[UInt128::_impl::little(0)]);
            const Float64 value_b = count_b.at(key.items[UInt128::_impl::little(1)]);

            assert(value_a > 0 && "frequency of value `a` must be positive");
            assert(value_b > 0 && "frequency of value `b` must be positive");

            const Float64 value_ab = value_ab_uint;

            sum += (value_ab / value_a) * (value_ab / value_b);
        }

        Float64 phi_squared = sum - 1.0;

        // Numerical errors might lead to a very small negative number
        if (phi_squared < 0.0)
        {
            assert(phi_squared > -1e-10);
            phi_squared = 0.0;
        }

        return phi_squared;
    }
};


template <typename Data>
class AggregateFunctionCrossTab final : public IAggregateFunctionDataHelper<Data, AggregateFunctionCrossTab<Data>>
{
public:
    explicit AggregateFunctionCrossTab(const DataTypes & arguments)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionCrossTab<Data>>({arguments}, {}, createResultType())
    {
    }

    String getName() const override { return Data::getName(); }

    bool allocatesMemoryInArena() const override { return false; }

    static DataTypePtr createResultType() { return std::make_shared<DataTypeNumber<Float64>>(); }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        UInt64 hash1 = UniqVariadicHash<false, false>::apply(1, &columns[0], row_num);
        UInt64 hash2 = UniqVariadicHash<false, false>::apply(1, &columns[1], row_num);

        this->data(place).add(hash1, hash2);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t>) const override
    {
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t>, Arena *) const override
    {
        this->data(place).deserialize(buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        Float64 result = this->data(place).getResult();
        auto & column = static_cast<ColumnVector<Float64> &>(to);
        column.getData().push_back(result);
    }
};

}
