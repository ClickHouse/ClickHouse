#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/CrossTab.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <memory>
#include <cmath>


namespace DB
{

namespace
{

struct TheilsUWindowData;

/// add() — O(1) with low constant factor
/// getResult() — O(|count_a| + |count_b| + |count_ab|)
/// Suitable to be used in GROUP BY
struct TheilsUData : CrossTabAggregateData
{
    static const char * getName()
    {
        return "theilsU";
    }

    using WindowData = TheilsUWindowData;

    using CrossTabAggregateData::merge;

    /// Merge window state into aggregation state. Window-specific cached fields are intentionally ignored.
    void merge(const TheilsUWindowData & other);

    /// Based on https://en.wikipedia.org/wiki/Uncertainty_coefficient.
    Float64 getResult() const
    {
        if (count < 2)
            return std::numeric_limits<Float64>::quiet_NaN();

        Float64 h_a = 0.0;
        for (const auto & [key, value] : count_a)
        {
            Float64 value_float = static_cast<Float64>(value);
            Float64 prob_a = value_float / static_cast<Float64>(count);
            h_a += prob_a * log(prob_a);
        }

        if (h_a == 0.0)
            return 0.0;

        Float64 dep = 0.0;
        for (const auto & [key, value] : count_ab)
        {
            Float64 value_ab = static_cast<Float64>(value);
            Float64 value_b = static_cast<Float64>(count_b.at(key.items[UInt128::_impl::little(1)]));
            Float64 prob_ab = value_ab / static_cast<Float64>(count);
            Float64 prob_a_given_b = value_ab / value_b;
            dep += prob_ab * log(prob_a_given_b);
        }

        Float64 coef = (h_a - dep) / h_a;
        return coef;
    }
};

/// add() - O(1) with high constant factor because of maintaining cached sums
/// getResult() - amortized O(1), independent of row/column degree
/// Suitable to be used in window functions (SELECT ... OVER(...) FROM ...)

/// Unlike others (e.g. cramersV, contingency), this class does not inherit from CrossTabPhiSquaredWindowData
/// because Theil's U incremental update can be done more efficiently without maintaining edges. Additionally,
/// CrossTabPhiSquaredWindowData suffers when there are high-degree rows/columns (graph is dense), leading to O(n) add()
/// complexity in worst case.
/// This implementation ensures add() is always O(1) regardless of data distribution.
struct TheilsUWindowData : CrossTabCountsState
{
    static const char * getName()
    {
        return TheilsUData::getName();
    }

    static constexpr CrossTabImplementationVariant state_representation = CrossTabImplementationVariant::Window;

    void add(UInt64 hash1, UInt64 hash2)
    {
        ++count;

        addToCountAndSum(count_a, hash1, 1, sum_a_nlogn);
        addToCountAndSum(count_b, hash2, 1, sum_b_nlogn);

        const UInt128 hash_pair{hash1, hash2};
        addToCountAndSum(count_ab, hash_pair, 1, sum_ab_nlogn);
    }

    void merge(const TheilsUWindowData & other)
    {
        if (other.count == 0)
            return;

        if (count == 0)
        {
            *this = other;
            return;
        }

        count += other.count;

        for (const auto & [key, add_value] : other.count_a)
            addToCountAndSum(count_a, key, add_value, sum_a_nlogn);

        for (const auto & [key, add_value] : other.count_b)
            addToCountAndSum(count_b, key, add_value, sum_b_nlogn);

        for (const auto & [key, add_value] : other.count_ab)
            addToCountAndSum(count_ab, key, add_value, sum_ab_nlogn);
    }

    void merge(const CrossTabAggregateData & other)
    {
        if (other.count == 0)
            return;

        if (count == 0)
        {
            count = other.count;
            count_a = other.count_a;
            count_b = other.count_b;
            count_ab = other.count_ab;

            /// Restore cached Σ n logn sums
            sum_a_nlogn = recomputeNLogNSum(count_a);
            sum_b_nlogn = recomputeNLogNSum(count_b);
            sum_ab_nlogn = recomputeNLogNSum(count_ab);
            return;
        }

        count += other.count;

        for (const auto & [key, add_value] : other.count_a)
            addToCountAndSum(count_a, key, add_value, sum_a_nlogn);

        for (const auto & [key, add_value] : other.count_b)
            addToCountAndSum(count_b, key, add_value, sum_b_nlogn);

        for (const auto & [key, add_value] : other.count_ab)
            addToCountAndSum(count_ab, key, add_value, sum_ab_nlogn);
    }

    /// Keep the same serialization format as CrossTabAggregateData
    void serialize(WriteBuffer & buf) const
    {
        writeBinary(count, buf);
        count_a.write(buf);
        count_b.write(buf);
        count_ab.write(buf);
    }

    void deserialize(ReadBuffer & buf)
    {
        clear();

        readBinary(count, buf);
        count_a.read(buf);
        count_b.read(buf);
        count_ab.read(buf);

        /// Restore cached Σ n logn sums
        sum_a_nlogn = recomputeNLogNSum(count_a);
        sum_b_nlogn = recomputeNLogNSum(count_b);
        sum_ab_nlogn = recomputeNLogNSum(count_ab);
    }

    Float64 getResult() const
    {
        if (count < 2)
            return std::numeric_limits<Float64>::quiet_NaN();

        const Float64 count_f = static_cast<Float64>(count);

        /// H(A) = log(N) - (Σ n_a log n_a) / N
        const Float64 h_a = std::log(count_f) - sum_a_nlogn / count_f;

        if (h_a <= 0.0)
            return 0.0;

        /// H(A|B) = (Σ n_b log n_b - Σ n_ab log n_ab) / N
        const Float64 h_a_given_b = (sum_b_nlogn - sum_ab_nlogn) / count_f;

        /// U(A|B) = 1 - H(A|B) / H(A)
        Float64 res = 1.0 - h_a_given_b / h_a;

        /// Clamp due to numerical error
        if (res < 0.0)
        {
            chassert(res > -1e-6);
            res = 0.0;
        }
        else if (res > 1.0)
        {
            chassert(res < 1.0 + 1e-6);
            res = 1.0;
        }

        return res;
    }

private:
    /// Σ n_a log n_a
    Float64 sum_a_nlogn = 0.0;

    /// Σ n_b log n_b
    Float64 sum_b_nlogn = 0.0;

    /// Σ n_ab log n_ab
    Float64 sum_ab_nlogn = 0.0;

    static Float64 nlogn(UInt64 x)
    {
        if (x <= 1)
            return 0.0;
        const Float64 xf = static_cast<Float64>(x);
        return xf * std::log(xf);
    }

    void clear()
    {
        count = 0;
        count_a.clear();
        count_b.clear();
        count_ab.clear();
        sum_a_nlogn = 0.0;
        sum_b_nlogn = 0.0;
        sum_ab_nlogn = 0.0;
    }

    template <typename Map, typename Key>
    static void addToCountAndSum(Map & map, const Key & key, UInt64 add_value, Float64 & sum_xlogx)
    {
        UInt64 & cur = map[key];
        const Float64 before = nlogn(cur);
        cur += add_value;
        sum_xlogx += nlogn(cur) - before;
    }

    template <typename Map>
    static Float64 recomputeNLogNSum(const Map & map)
    {
        /// Kahan summation to reduce numerical error
        Float64 sum = 0.0;
        Float64 c = 0.0;

        for (const auto & [_, value] : map)
        {
            const Float64 term = nlogn(value);
            const Float64 y = term - c;
            const Float64 t = sum + y;
            c = (t - sum) - y;
            sum = t;
        }

        return sum;
    }
};

void TheilsUData::merge(const TheilsUWindowData & other)
{
    CrossTabCountsState::merge(static_cast<const CrossTabCountsState &>(other));
}
}

void registerAggregateFunctionTheilsU(AggregateFunctionFactory & factory)
{
    FunctionDocumentation::Description description = R"(
The `theilsU` function calculates the [Theil's U uncertainty coefficient](https://en.wikipedia.org/wiki/Contingency_table#Uncertainty_coefficient), a value that measures the association between two columns in a table.
Its values range from 0.0 (no association) to 1.0 (perfect agreement).
    )";
    FunctionDocumentation::Syntax syntax = R"(
theilsU(column1, column2)
    )";
    FunctionDocumentation::Arguments arguments = {
        {"column1", "First column to be compared.", {"Any"}},
        {"column2", "Second column to be compared.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {
        "Returns a value between 0 and 1.", {"Float64"}
    };
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
SELECT theilsU(a, b)
FROM (
    SELECT
        number % 10 AS a,
        number % 4 AS b
    FROM
        numbers(150)
);
        )",
        R"(
┌────────theilsU(a, b)─┐
│  0.30195720557678846 │
└──────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {22, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::AggregateFunction;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction(TheilsUData::getName(), {
        [](const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            assertBinary(name, argument_types);
            assertNoParameters(name, parameters);
            return std::make_shared<AggregateFunctionCrossTab<TheilsUData>>(argument_types);
        },
        {},
        documentation,
        [](const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            assertBinary(name, argument_types);
            assertNoParameters(name, parameters);
            return std::make_shared<AggregateFunctionCrossTab<TheilsUWindowData>>(argument_types);
        }});
}

}
