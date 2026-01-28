#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/UniqVariadicHash.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/HashTable/HashMap.h>
#include <Common/assert_cast.h>

#include <type_traits>


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

enum class CrossTabImplementationVariant : UInt8
{
    Aggregation,
    Window
};

struct CrossTabPhiSquaredWindowData;

/// Common (count + maps) state layout.
/// It is used as the aggregation state (`CrossTabAggregateData`) and also as a prefix for some window states
/// that keep the same base layout but add extra cached fields (e.g. Theil's U).
struct CrossTabCountsState
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

    void merge(const CrossTabCountsState & other)
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
};

struct CrossTabAggregateData : CrossTabCountsState
{
    static constexpr CrossTabImplementationVariant state_representation = CrossTabImplementationVariant::Aggregation;

    using CrossTabCountsState::add;
    using CrossTabCountsState::deserialize;
    using CrossTabCountsState::merge;
    using CrossTabCountsState::serialize;

    void merge(const CrossTabPhiSquaredWindowData & other);

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
            const Float64 value_a = static_cast<Float64>(count_a.at(key.items[UInt128::_impl::little(0)]));
            const Float64 value_b = static_cast<Float64>(count_b.at(key.items[UInt128::_impl::little(1)]));

            chassert(value_a > 0 && "frequency of value `a` must be positive");
            chassert(value_b > 0 && "frequency of value `b` must be positive");

            const Float64 value_ab = static_cast<Float64>(value_ab_uint);

            /// (ab/a) * (ab/b) == ab^2 / (a * b), but with one division
            sum += (value_ab * value_ab) / (value_a * value_b);
        }

        Float64 phi_squared = sum - 1.0;

        /// Numerical errors might lead to a very small negative number
        chassert(phi_squared > -1e-6);
        phi_squared = std::max(phi_squared, 0.0);

        /// Theoretical bound: phi^2 <= min(|A|, |B|) - 1
        const UInt64 q = std::min<UInt64>(count_a.size(), count_b.size());
        if (q <= 1)
            return 0.0;

        const Float64 max_phi_squared = static_cast<Float64>(q - 1);
        chassert(phi_squared <= max_phi_squared + 1e-6);

        phi_squared = std::min(phi_squared, max_phi_squared);

        return phi_squared;
    }
};


/// add() — amortized O(deg(a) + deg(b)) because cached `phi_term_sum` is updated by iterating all non-zero cells
///         in the affected row/column (deg(x) = number of distinct (a,b) pairs incident to that value).
/// getPhiSquared() — O(1)
/// Suitable for window functions (many getResult()/getPhiSquared() calls between updates).
/// Window state implementation that maintains φ² (phi-squared) incrementally.
///
/// It stores the contingency table as a bipartite graph: distinct `a` and `b` values are nodes, and each observed (a, b)
/// pair is an edge with its count. This representation allows efficient iteration over all pairs that share the same `a`
/// (row) or the same `b` (column), which is required to update φ² in add()/merge().
///
/// This base is used by window-friendly implementations of `cramersV`, `cramersVBiasCorrected`, and `contingency`.
///
/// Performance characteristics:
/// - Fast when the joint distribution is sparse (each `a` is seen with only a few `b` values and vice versa),
///   e.g. functional or near-functional relationships.
/// - Can be slow when the joint distribution is dense or highly skewed (some values have very large degree),
///   because each add() touches all incident edges. In the worst case deg(x) can grow to O(#distinct on the other side),
///   making add() up to O(N) over the lifetime of the window.
///
/// For example, the following is a worst-case (dense K × K grid, each row/column degree ≈ K):
/// SELECT
///     cramersV(a, b) OVER (ORDER BY number) AS v
/// FROM
/// (
///     SELECT
///         number,
///         toUInt32(number % 1000) AS a,
///         toUInt32(intDiv(number, 1000) % 1000) AS b
///     FROM numbers(1000 * 1000)
/// );
/// This generates an almost complete 1000×1000 set of (a,b) pairs, so each add() processes ~1000 incident edges,
/// which will be very slow.
struct CrossTabPhiSquaredWindowData
{
    static constexpr CrossTabImplementationVariant state_representation = CrossTabImplementationVariant::Window;

    struct Edge
    {
        UInt32 a;
        UInt32 b;
        UInt64 count;
    };

    /// Total count
    UInt64 count = 0;

    /// Cached Σ n_ab^2/(n_a n_b)
    /// phi^2 = phi_term_sum - 1
    /// Keeping this variable up-to-date is the main purpose of this class
    Float64 phi_term_sum = 0.0;

    /// To maintain `phi_term_sum` incrementally, when a new (a, b) observation arrives we must update
    /// all terms that depend on the affected marginals `n_a` and `n_b`. That requires fast iteration
    /// over all existing pairs that share the same `a` (row) or the same `b` (column), so we keep
    /// adjacency lists `a_incident_edges` and `b_incident_edges`.
    ///
    /// This could be implemented with additional hash maps (e.g. hash -> list of pairs), but that would
    /// increase memory usage and add extra hash-table lookups/overhead on every update.
    ///
    /// We do a small number of hash-table lookups to map `hash1`/`hash2` to compact indices in add()/merge().
    /// After that, all hot-path work (marginals, adjacency traversal, and phi_term_sum updates) operates on
    /// dense arrays/vectors, which is faster and more cache-friendly than repeated hash lookups.


    /// Maps from hashed value -> compact index in arrays (a_hash_by_index[], a_marginal_count[], a_incident_edges[])
    HashMapWithStackMemory<UInt64, UInt32, TrivialHash, 4> a_index_by_hash;
    HashMapWithStackMemory<UInt64, UInt32, TrivialHash, 4> b_index_by_hash;

    /// Map from (hash_a, hash_b) -> edge index in ab_edges[]
    HashMapWithStackMemory<UInt128, UInt32, UInt128Hash, 4> ab_edge_index_by_pair;

    /// Reverse maps index (compact index -> hash)
    std::vector<UInt64> a_hash_by_index;
    std::vector<UInt64> b_hash_by_index;

    /// compact index -> marginal counts
    std::vector<UInt64> a_marginal_count;
    std::vector<UInt64> b_marginal_count;

    /// Incident edge lists by node
    /// compact index -> edge indices (in ab_edges[])
    std::vector<std::vector<UInt32>> a_incident_edges;
    std::vector<std::vector<UInt32>> b_incident_edges;

    /// All observed (a, b) pairs as edges
    std::vector<Edge> ab_edges;

    /// Marks for "a" nodes used during merge() to avoid double-subtract/add.
    /// During a merge we first adjust affected rows (all edges with affected `a`), then adjust affected
    /// columns but must skip edges whose `a` was already handled by the row pass.
    /// We mark affected `a` indices by setting `a_epoch_mark[a_idx] = epoch`. Since `epoch` is unique
    /// for the current merge, we can test membership with a single integer compare without clearing the
    /// mark array each time.
    std::vector<UInt32> a_epoch_mark;
    UInt32 epoch = 1;

    void add(UInt64 hash1, UInt64 hash2)
    {
        const UInt32 a_idx = getOrCreateIndexA(hash1);
        const UInt32 b_idx = getOrCreateIndexB(hash2);

        const UInt64 old_count_a = a_marginal_count[a_idx];
        const UInt64 old_count_b = b_marginal_count[b_idx];

        const UInt128 pair_key{hash1, hash2};
        auto * it_edge = ab_edge_index_by_pair.find(pair_key);
        const bool has_edge = (it_edge != ab_edge_index_by_pair.end());
        UInt32 edge_idx = has_edge ? it_edge->getMapped() : INVALID_EDGE_IDX;

        /// Remove old contributions for the affected row/column
        if (old_count_a)
            applyRowPhiDelta(a_idx, static_cast<Float64>(old_count_a), -1.0);

        if (old_count_b)
            applyColumnPhiDelta(b_idx, static_cast<Float64>(old_count_b), -1.0, has_edge ? edge_idx : INVALID_EDGE_IDX);

        /// Apply the update
        ++count;
        ++a_marginal_count[a_idx];
        ++b_marginal_count[b_idx];

        if (has_edge)
        {
            ++ab_edges[edge_idx].count;
        }
        else
        {
            edge_idx = static_cast<UInt32>(ab_edges.size());
            ab_edges.push_back(Edge{a_idx, b_idx, 1});
            ab_edge_index_by_pair[pair_key] = edge_idx;
            a_incident_edges[a_idx].push_back(edge_idx);
            b_incident_edges[b_idx].push_back(edge_idx);
        }

        /// Add new contributions for the affected row/column
        applyRowPhiDelta(a_idx, static_cast<Float64>(a_marginal_count[a_idx]), +1.0);

        /// (a, b) term is already accounted in row update above
        applyColumnPhiDelta(b_idx, static_cast<Float64>(b_marginal_count[b_idx]), +1.0, edge_idx);
    }

    void merge(const CrossTabPhiSquaredWindowData & other)
    {
        if (other.count == 0)
            return;

        /// Remap other indices -> our indices
        std::vector<UInt32> map_a(other.a_hash_by_index.size());
        std::vector<UInt32> map_b(other.b_hash_by_index.size());

        /// Keep the affected node indices (in our indexing)
        std::vector<UInt32> affected_a;
        affected_a.reserve(other.a_hash_by_index.size());
        for (size_t i = 0; i < other.a_hash_by_index.size(); ++i)
        {
            map_a[i] = getOrCreateIndexA(other.a_hash_by_index[i]);
            affected_a.push_back(map_a[i]);
        }

        std::vector<UInt32> affected_b;
        affected_b.reserve(other.b_hash_by_index.size());
        for (size_t i = 0; i < other.b_hash_by_index.size(); ++i)
        {
            map_b[i] = getOrCreateIndexB(other.b_hash_by_index[i]);
            affected_b.push_back(map_b[i]);
        }

        const UInt32 cur_epoch = epoch;
        prepareMerge(affected_a, affected_b, cur_epoch);

        /// Apply count changes
        count += other.count;

        for (size_t i = 0; i < other.a_marginal_count.size(); ++i)
            a_marginal_count[map_a[i]] += other.a_marginal_count[i];

        for (size_t i = 0; i < other.b_marginal_count.size(); ++i)
            b_marginal_count[map_b[i]] += other.b_marginal_count[i];


        /// Merge edges (joint counts)
        for (const auto & edge : other.ab_edges)
        {
            const UInt64 hash_a = other.a_hash_by_index[edge.a];
            const UInt64 hash_b = other.b_hash_by_index[edge.b];

            const UInt32 a_idx = map_a[edge.a];
            const UInt32 b_idx = map_b[edge.b];

            const UInt128 pair_key{hash_a, hash_b};
            auto * it = ab_edge_index_by_pair.find(pair_key);
            if (it != ab_edge_index_by_pair.end())
            {
                ab_edges[it->getMapped()].count += edge.count;
            }
            else
            {
                const UInt32 new_edge_idx = static_cast<UInt32>(ab_edges.size());
                ab_edges.push_back(Edge{a_idx, b_idx, edge.count});
                ab_edge_index_by_pair[pair_key] = new_edge_idx;
                a_incident_edges[a_idx].push_back(new_edge_idx);
                b_incident_edges[b_idx].push_back(new_edge_idx);
            }
        }

        finalizeMerge(affected_a, affected_b, cur_epoch);
    }

    /// Merge a CrossTabAggregateData (GROUP BY) state into the window-optimized representation.
    /// This is used to support merging "aggregate" states in OVER()
    void merge(const CrossTabAggregateData & other)
    {
        if (other.count == 0)
            return;

        std::vector<UInt32> affected_a;
        affected_a.reserve(other.count_a.size());
        for (const auto & [hash, _] : other.count_a)
            affected_a.push_back(getOrCreateIndexA(hash));

        std::vector<UInt32> affected_b;
        affected_b.reserve(other.count_b.size());
        for (const auto & [hash, _] : other.count_b)
            affected_b.push_back(getOrCreateIndexB(hash));

        const UInt32 cur_epoch = epoch;
        prepareMerge(affected_a, affected_b, cur_epoch);

        /// Apply count changes
        count += other.count;
        for (const auto & [hash, cnt] : other.count_a)
            a_marginal_count[a_index_by_hash.at(hash)] += cnt;
        for (const auto & [hash, cnt] : other.count_b)
            b_marginal_count[b_index_by_hash.at(hash)] += cnt;

        /// Merge edges (joint counts)
        for (const auto & [pair_key, cnt_ab] : other.count_ab)
        {
            const UInt64 hash_a = pair_key.items[UInt128::_impl::little(0)];
            const UInt64 hash_b = pair_key.items[UInt128::_impl::little(1)];

            const UInt32 a_idx = a_index_by_hash.at(hash_a);
            const UInt32 b_idx = b_index_by_hash.at(hash_b);

            auto * it = ab_edge_index_by_pair.find(pair_key);
            if (it != ab_edge_index_by_pair.end())
            {
                ab_edges[it->getMapped()].count += cnt_ab;
            }
            else
            {
                const UInt32 edge_idx = static_cast<UInt32>(ab_edges.size());
                ab_edges.push_back(Edge{a_idx, b_idx, cnt_ab});
                ab_edge_index_by_pair[pair_key] = edge_idx;
                a_incident_edges[a_idx].push_back(edge_idx);
                b_incident_edges[b_idx].push_back(edge_idx);
            }
        }

        finalizeMerge(affected_a, affected_b, cur_epoch);
    }

    /// Keep the same serialization format as CrossTabAggregateData
    void serialize(WriteBuffer & buf) const
    {
        writeBinary(count, buf);

        HashMapWithStackMemory<UInt64, UInt64, TrivialHash, 4> count_a;
        for (size_t i = 0; i < a_hash_by_index.size(); ++i)
            count_a[a_hash_by_index[i]] = a_marginal_count[i];
        count_a.write(buf);

        HashMapWithStackMemory<UInt64, UInt64, TrivialHash, 4> count_b;
        for (size_t i = 0; i < b_hash_by_index.size(); ++i)
            count_b[b_hash_by_index[i]] = b_marginal_count[i];
        count_b.write(buf);

        HashMapWithStackMemory<UInt128, UInt64, UInt128Hash, 4> count_ab;
        for (const auto & edge : ab_edges)
        {
            const UInt128 pair_key{a_hash_by_index[edge.a], b_hash_by_index[edge.b]};
            count_ab[pair_key] = edge.count;
        }
        count_ab.write(buf);
    }

    void deserialize(ReadBuffer & buf)
    {
        clear();

        readBinary(count, buf);

        HashMapWithStackMemory<UInt64, UInt64, TrivialHash, 4> count_a;
        HashMapWithStackMemory<UInt64, UInt64, TrivialHash, 4> count_b;
        HashMapWithStackMemory<UInt128, UInt64, UInt128Hash, 4> count_ab;

        count_a.read(buf);
        count_b.read(buf);
        count_ab.read(buf);

        /// Update the internal states
        a_hash_by_index.reserve(count_a.size());
        a_marginal_count.reserve(count_a.size());
        a_incident_edges.reserve(count_a.size());
        a_epoch_mark.reserve(count_a.size());

        for (const auto & [hash, cnt] : count_a)
        {
            const UInt32 idx = static_cast<UInt32>(a_hash_by_index.size());
            a_index_by_hash[hash] = idx;
            a_hash_by_index.push_back(hash);
            a_marginal_count.push_back(cnt);
            a_incident_edges.emplace_back();

            a_epoch_mark.push_back(0);
        }

        epoch = 1;

        b_hash_by_index.reserve(count_b.size());
        b_marginal_count.reserve(count_b.size());
        b_incident_edges.reserve(count_b.size());

        for (const auto & [hash, cnt] : count_b)
        {
            const UInt32 idx = static_cast<UInt32>(b_hash_by_index.size());
            b_index_by_hash[hash] = idx;
            b_hash_by_index.push_back(hash);
            b_marginal_count.push_back(cnt);
            b_incident_edges.emplace_back();
        }

        /// Additionally, compute `phi_term_sum` on the fly
        ab_edges.reserve(count_ab.size());
        phi_term_sum = 0.0;

        for (const auto & [pair_key, cnt_ab] : count_ab)
        {
            const UInt64 hash_a = pair_key.items[UInt128::_impl::little(0)];
            const UInt64 hash_b = pair_key.items[UInt128::_impl::little(1)];

            const UInt32 a_idx = a_index_by_hash.at(hash_a);
            const UInt32 b_idx = b_index_by_hash.at(hash_b);

            const UInt32 edge_idx = static_cast<UInt32>(ab_edges.size());
            ab_edges.push_back(Edge{a_idx, b_idx, cnt_ab});

            ab_edge_index_by_pair[pair_key] = edge_idx;
            a_incident_edges[a_idx].push_back(edge_idx);
            b_incident_edges[b_idx].push_back(edge_idx);

            const Float64 a = static_cast<Float64>(a_marginal_count[a_idx]);
            const Float64 b = static_cast<Float64>(b_marginal_count[b_idx]);
            phi_term_sum += phiTerm(cnt_ab, a, b);
        }
    }

    Float64 getPhiSquared() const
    {
        if (count == 0)
            return 0.0;

        const UInt64 q = std::min<UInt64>(a_marginal_count.size(), b_marginal_count.size());
        if (q <= 1)
            return 0.0;

        Float64 phi_squared = phi_term_sum - 1.0;

        /// Numerical errors might lead to a very small negative number
        chassert(phi_squared > -1e-6);
        phi_squared = std::max(phi_squared, 0.0);

        /// Theoretical bound: phi^2 <= min(|A|, |B|) - 1
        /// Incremental updates can drift slightly above it; clamp to avoid inf/invalid results
        const Float64 max_phi_squared = static_cast<Float64>(q - 1);
        chassert(phi_squared <= max_phi_squared + 1e-6);

        phi_squared = std::min(phi_squared, max_phi_squared);

        return phi_squared;
    }

private:
    static constexpr UInt32 INVALID_EDGE_IDX = UInt32(-1);

    void prepareMerge(const std::vector<UInt32> & affected_a, const std::vector<UInt32> & affected_b, UInt32 cur_epoch)
    {
        for (UInt32 a_idx : affected_a)
            a_epoch_mark[a_idx] = cur_epoch;

        /// Remove old contributions for affected rows (all edges in those rows)
        for (UInt32 a_idx : affected_a)
        {
            const UInt64 old_a = a_marginal_count[a_idx];
            if (old_a)
                applyRowPhiDelta(a_idx, static_cast<Float64>(old_a), -1.0);
        }

        /// Remove old contributions for affected columns, but only edges whose 'a' is NOT affected.
        ///    Otherwise, we would double-subtract those edges
        for (UInt32 b_idx : affected_b)
        {
            const UInt64 old_b = b_marginal_count[b_idx];
            if (old_b)
                adjustColumnPhiSkipMarkedA(b_idx, static_cast<Float64>(old_b), -1.0, cur_epoch);
        }
    }

    void finalizeMerge(const std::vector<UInt32> & affected_a, const std::vector<UInt32> & affected_b, UInt32 cur_epoch)
    {
        /// Add new contributions back for affected rows
        for (UInt32 a_idx : affected_a)
        {
            const UInt64 new_a = a_marginal_count[a_idx];
            if (new_a)
                applyRowPhiDelta(a_idx, static_cast<Float64>(new_a), +1.0);
        }

        /// Add new contributions back for affected columns, skipping edges whose 'a' is affected.
        ///  Otherwise, we would double-add those edges
        for (UInt32 b_idx : affected_b)
        {
            const UInt64 new_b = b_marginal_count[b_idx];
            if (new_b)
                adjustColumnPhiSkipMarkedA(b_idx, static_cast<Float64>(new_b), +1.0, cur_epoch);
        }

        ++epoch;

        /// For theoretical correctness, we should now handle the case epoch == 0 by resetting all marks to 0 and setting epoch = 1.
        /// This will require UINT32_MAX merges which is impossible in practice, so we skip for simplicity.
    }

    void clear()
    {
        count = 0;
        phi_term_sum = 0.0;

        a_index_by_hash.clear();
        b_index_by_hash.clear();
        ab_edge_index_by_pair.clear();

        a_hash_by_index.clear();
        b_hash_by_index.clear();
        a_marginal_count.clear();
        b_marginal_count.clear();
        a_incident_edges.clear();
        b_incident_edges.clear();
        ab_edges.clear();

        a_epoch_mark.clear();
        epoch = 1;
    }

    static Float64 phiTerm(UInt64 ab_count, Float64 a, Float64 b)
    {
        const Float64 ab = static_cast<Float64>(ab_count);
        return (ab * ab) / (a * b); /// same as (ab/a) * (ab/b) but faster with only one division
    }

    void applyRowPhiDelta(UInt32 a_idx, Float64 a, Float64 sign)
    {
        for (UInt32 e : a_incident_edges[a_idx])
        {
            const auto & edge = ab_edges[e];
            const Float64 b = static_cast<Float64>(b_marginal_count[edge.b]);
            phi_term_sum += sign * phiTerm(edge.count, a, b);
        }
    }

    /// skip_edge == INVALID_EDGE_IDX means no edge should be skipped
    void applyColumnPhiDelta(UInt32 b_idx, Float64 b, Float64 sign, UInt32 skip_edge)
    {
        for (UInt32 e : b_incident_edges[b_idx])
        {
            if (e == skip_edge)
                continue;

            const auto & edge = ab_edges[e];
            const Float64 a = static_cast<Float64>(a_marginal_count[edge.a]);
            phi_term_sum += sign * phiTerm(edge.count, a, b);
        }
    }

    UInt32 getOrCreateIndexA(UInt64 hash)
    {
        auto * it = a_index_by_hash.find(hash);
        if (it != a_index_by_hash.end())
            return it->getMapped();

        const UInt32 idx = static_cast<UInt32>(a_hash_by_index.size());
        a_index_by_hash[hash] = idx;
        a_hash_by_index.push_back(hash);
        a_marginal_count.push_back(0);
        a_incident_edges.emplace_back();

        a_epoch_mark.push_back(0);
        return idx;
    }

    UInt32 getOrCreateIndexB(UInt64 hash)
    {
        auto * it = b_index_by_hash.find(hash);
        if (it != b_index_by_hash.end())
            return it->getMapped();

        const UInt32 idx = static_cast<UInt32>(b_hash_by_index.size());
        b_index_by_hash[hash] = idx;
        b_hash_by_index.push_back(hash);
        b_marginal_count.push_back(0);
        b_incident_edges.emplace_back();
        return idx;
    }

    void adjustColumnPhiSkipMarkedA(UInt32 b_idx, Float64 b, Float64 sign, UInt32 cur_epoch)
    {
        for (UInt32 e : b_incident_edges[b_idx])
        {
            const auto & edge = ab_edges[e];

            /// This edge is already handled in row update
            if (a_epoch_mark[edge.a] == cur_epoch)
                continue;

            const Float64 a = static_cast<Float64>(a_marginal_count[edge.a]);
            phi_term_sum += sign * phiTerm(edge.count, a, b);
        }
    }
};

inline void CrossTabAggregateData::merge(const CrossTabPhiSquaredWindowData & other)
{
    if (other.count == 0)
        return;

    count += other.count;

    for (size_t i = 0; i < other.a_hash_by_index.size(); ++i)
        count_a[other.a_hash_by_index[i]] += other.a_marginal_count[i];

    for (size_t i = 0; i < other.b_hash_by_index.size(); ++i)
        count_b[other.b_hash_by_index[i]] += other.b_marginal_count[i];

    for (const auto & edge : other.ab_edges)
    {
        const UInt128 pair_key{other.a_hash_by_index[edge.a], other.b_hash_by_index[edge.b]};
        count_ab[pair_key] += edge.count;
    }
}


template <typename Data>
class AggregateFunctionCrossTab final : public IAggregateFunctionDataHelper<Data, AggregateFunctionCrossTab<Data>>
{
    static_assert(
        Data::state_representation != CrossTabImplementationVariant::Window
            || std::is_base_of_v<CrossTabPhiSquaredWindowData, Data> || std::is_base_of_v<CrossTabCountsState, Data>,
        "CrossTab window state must either derive from CrossTabPhiSquaredWindowData or keep CrossTabCountsState prefix (derive from "
        "CrossTabCountsState).");

    static_assert(
        Data::state_representation != CrossTabImplementationVariant::Window
            || !std::is_base_of_v<CrossTabPhiSquaredWindowData, Data> || sizeof(Data) == sizeof(CrossTabPhiSquaredWindowData),
        "CrossTab window state derived from CrossTabPhiSquaredWindowData must not add data members; it is merged as "
        "CrossTabPhiSquaredWindowData.");

    static_assert(
        Data::state_representation != CrossTabImplementationVariant::Aggregation || std::is_base_of_v<CrossTabAggregateData, Data>,
        "CrossTab aggregation state must derive from CrossTabAggregateData.");

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

    bool canMergeStateFromDifferentVariant(const IAggregateFunction & rhs) const override
    {
        if (rhs.getName() != getName())
            return false;

        if (rhs.getStateVariant() == getStateVariant())
            return false;

        if constexpr (Data::state_representation == CrossTabImplementationVariant::Window)
        {
            /// Merge aggregation states into the window representation.
            /// The aggregation representation for this family is CrossTabAggregateData.
            if (rhs.getStateVariant() != AggregateFunctionStateVariant::Aggregation)
                return false;

            return true;
        }
        else if constexpr (Data::state_representation == CrossTabImplementationVariant::Aggregation)
        {
            /// Merge window states into the aggregation representation.
            ///
            /// There are multiple window representations:
            ///  - CrossTabPhiSquaredWindowData based (cramersV, cramersVBiasCorrected, contingency).
            ///  - CrossTabCountsState prefix based (window states that keep the same (count, maps) layout and may add cached fields).
            if (rhs.getStateVariant() != AggregateFunctionStateVariant::Window)
                return false;

            return true;
        }
        else
        {
            return false;
        }
    }

    void mergeStateFromDifferentVariant(
        AggregateDataPtr __restrict place, const IAggregateFunction & rhs, ConstAggregateDataPtr rhs_place, Arena *) const override
    {
        chassert(canMergeStateFromDifferentVariant(rhs));

        if constexpr (Data::state_representation == CrossTabImplementationVariant::Window)
        {
            auto & dst = this->data(place);
            const auto & src = *reinterpret_cast<const CrossTabAggregateData *>(rhs_place);
            dst.merge(src);
        }
        else if constexpr (Data::state_representation == CrossTabImplementationVariant::Aggregation)
        {
            auto & dst = this->data(place);

            /// By default, window state for this family is CrossTabPhiSquaredWindowData-based.
            /// If a specific aggregate function has a different window state layout, it can declare:
            ///   using WindowData = <its window state type>;
            ///   void merge(const WindowData &);
            if constexpr (requires { typename Data::WindowData; }) /// Only TheilsUData declares it
            {
                using WindowData = typename Data::WindowData;
                static_assert(
                    std::is_base_of_v<CrossTabCountsState, WindowData>,
                    "CrossTab custom window state must keep CrossTabCountsState prefix (derive from CrossTabCountsState).");
                static_assert(requires(Data & data, const WindowData & other) { data.merge(other); });

                const auto & src = *reinterpret_cast<const WindowData *>(rhs_place);
                dst.merge(src);
            }
            else
            {
                const auto & src = *reinterpret_cast<const CrossTabPhiSquaredWindowData *>(rhs_place);
                dst.merge(src);
            }
        }
        else
        {
            static_assert(
                std::is_same_v<Data, void>,
                "AggregateFunctionCrossTab::mergeStateFromDifferentVariant is implemented only for Data types with state_representation");
        }
    }

    AggregateFunctionStateVariant getStateVariant() const override
    {
        if constexpr (Data::state_representation == CrossTabImplementationVariant::Window)
            return AggregateFunctionStateVariant::Window;
        return AggregateFunctionStateVariant::Aggregation;
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        Float64 result = this->data(place).getResult();
        auto & column = static_cast<ColumnVector<Float64> &>(to);
        column.getData().push_back(result);
    }
};

}
