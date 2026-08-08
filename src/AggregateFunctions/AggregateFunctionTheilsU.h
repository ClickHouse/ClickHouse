#pragma once

#include <AggregateFunctions/CrossTab.h>

#include <cmath>


namespace DB
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
        return computeExact(*this);
    }

    /// Computes the entropies directly from the count maps: a constant first argument
    /// gives H(A) = 0 exactly (every term is `1 · log 1`), and a genuinely small H(A)
    /// does not suffer from the catastrophic cancellation of the cached-sums formula
    /// in `TheilsUWindowData::getResult`, which uses this as a fallback.
    static Float64 computeExact(const CrossTabCountsState & state)
    {
        if (state.count < 2)
            return std::numeric_limits<Float64>::quiet_NaN();

        Float64 h_a = 0.0;
        for (const auto & [key, value] : state.count_a)
        {
            Float64 value_float = static_cast<Float64>(value);
            Float64 prob_a = value_float / static_cast<Float64>(state.count);
            h_a += prob_a * log(prob_a);
        }

        if (h_a == 0.0)
            return 0.0;

        Float64 dep = 0.0;
        for (const auto & [key, value] : state.count_ab)
        {
            Float64 value_ab = static_cast<Float64>(value);
            Float64 value_b = static_cast<Float64>(state.count_b.at(key.items[UInt128::_impl::little(1)]));
            Float64 prob_ab = value_ab / static_cast<Float64>(state.count);
            Float64 prob_a_given_b = value_ab / value_b;
            dep += prob_ab * log(prob_a_given_b);
        }

        Float64 coef = (h_a - dep) / h_a;
        return coef;
    }
};

/// add() - O(1) with high constant factor because of maintaining cached sums
/// getResult() - amortized O(1), independent of row/column degree
///               (except for frames with a constant first argument, which take an O(|count_a|) = O(1) shortcut)
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

    static constexpr AggregateFunctionStateVariant state_representation = AggregateFunctionStateVariant::Window;

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
        CrossTabCountsState::deserialize(buf);

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
        const Float64 h_a = std::log(count_f) - sum_a_nlogn.get() / count_f;

        /// The cached Σ n·log n sums are kept in compensated form, so their error
        /// does not accumulate over the incremental updates: the per-key `n log n`
        /// evaluations telescope exactly (the same rounded value enters the sum with
        /// `+` and later leaves it with `-`), and the compensation captures the
        /// rounding residual of every addition to the running sum. What remains is
        /// O(ε · log N) absolute noise in the derived entropies (the rounding of the
        /// per-update deltas, the final `sum + compensation`, and the division).
        /// When the computed H(A) is at or below that noise level, the true H(A)
        /// is zero for every attainable frame size (a genuinely non-constant first
        /// argument gives H(A) ≥ log(N) / N, which stays above the bound until
        /// N · ε ≈ 32): the first argument is constant within the frame, and
        /// `1 - H(A|B) / H(A)` would divide noise by noise. Take the exact
        /// recomputation shortcut in that case: with a constant first argument it
        /// returns 0 after scanning only the single-entry `count_a` map (every term
        /// is `1 · log 1`), without touching `count_ab`, so the amortized O(1)
        /// complexity is preserved. Widen the sanity-check tolerance on the fast
        /// path by the same relative error bound.
        const Float64 entropy_error = 32 * std::numeric_limits<Float64>::epsilon() * std::log(count_f);

        if (h_a <= entropy_error)
            return TheilsUData::computeExact(*this);

        /// H(A|B) = (Σ n_b log n_b - Σ n_ab log n_ab) / N
        const Float64 h_a_given_b = (sum_b_nlogn.get() - sum_ab_nlogn.get()) / count_f;

        /// U(A|B) = 1 - H(A|B) / H(A)
        Float64 res = 1.0 - h_a_given_b / h_a;

        /// Clamp due to numerical error
        const Float64 tolerance = 1e-4 + entropy_error / h_a;
        if (res < 0.0)
        {
            chassert(res > -tolerance);
            res = 0.0;
        }
        else if (res > 1.0)
        {
            chassert(res < 1.0 + tolerance);
            res = 1.0;
        }

        return res;
    }

private:
    /// Neumaier-compensated running sum: `add` captures the exact rounding residual
    /// of every addition, so the error of the accumulated value does not grow with
    /// the number of additions. Without the compensation, the plain running sums
    /// accumulate O(ε · N · log N) error over N incremental updates, which swamps
    /// the O(log N / N) entropy of an almost-constant column already at N ≈ 2.4e7
    /// and makes `getResult` unable to distinguish it from an exactly constant one.
    struct CompensatedSum
    {
        Float64 sum = 0.0;
        Float64 compensation = 0.0;

        void add(Float64 value)
        {
            const Float64 t = sum + value;
            if (std::abs(sum) >= std::abs(value))
                compensation += (sum - t) + value;
            else
                compensation += (value - t) + sum;
            sum = t;
        }

        Float64 get() const
        {
            return sum + compensation;
        }
    };

    /// Σ n_a log n_a
    CompensatedSum sum_a_nlogn;

    /// Σ n_b log n_b
    CompensatedSum sum_b_nlogn;

    /// Σ n_ab log n_ab
    CompensatedSum sum_ab_nlogn;

    static Float64 nlogn(UInt64 x)
    {
        if (x <= 1)
            return 0.0;
        const Float64 xf = static_cast<Float64>(x);
        return xf * std::log(xf);
    }

    template <typename Map, typename Key>
    static void addToCountAndSum(Map & map, const Key & key, UInt64 add_value, CompensatedSum & sum_xlogx)
    {
        UInt64 & cur = map[key];
        const Float64 before = nlogn(cur);
        cur += add_value;
        sum_xlogx.add(nlogn(cur) - before);
    }

    template <typename Map>
    static CompensatedSum recomputeNLogNSum(const Map & map)
    {
        CompensatedSum sum;
        for (const auto & [_, value] : map)
            sum.add(nlogn(value));
        return sum;
    }
};

inline void TheilsUData::merge(const TheilsUWindowData & other)
{
    CrossTabCountsState::merge(static_cast<const CrossTabCountsState &>(other));
}

}
