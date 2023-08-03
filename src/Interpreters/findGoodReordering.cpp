#include <Interpreters/findGoodReordering.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static std::vector<Float64> ones(size_t N)
{
    std::vector<Float64> vec(N);
    for (size_t i = 0; i < N; ++i)
        vec[i] = 1.0;
    return vec;
}

static std::vector<size_t> range(size_t N)
{
    /// returns numbers from 0 up to N, excluding N.
    std::vector<size_t> indexes(N);
    for (size_t i = 0; i < N; ++i)
        indexes[i] = i;
    return indexes;
}

static size_t removeElems(size_t n, std::vector<size_t> & elems_to_remove, Vector2D<Float64> & selectivities, std::vector<Float64> & selectivity_to_joined, std::vector<Float64> & self_selectivities, std::vector<Float64> & sizes, std::vector<size_t> & indexes)
{
    /// We do "removal" of indexes from the data structures used by greedy() by
    ///     1) copying the element at the back of the data structure, at index n-1, to the index to be removed.
    ///     2) decrementing n.
    for (size_t i = 0; i < elems_to_remove.size(); ++i)
    {
        size_t idx_to_remove = elems_to_remove[i];

        if (i > 0)
            idx_to_remove = std::find(indexes.begin(), indexes.end(), idx_to_remove) - indexes.begin();

        //node idx_to_remove is now a part of the join tree, update selectivities for the remaining nodes.
        for (size_t k = 0; k < n; k++)
        {
            selectivity_to_joined[k] *= selectivities(idx_to_remove, k);
        }

        //move last node (index: n-1) to the place of selected node (index: idx_to_remove).
        if (idx_to_remove != n-1)
        {
            for (size_t k = 0; k < n; k++)
            {
                selectivities(k, idx_to_remove) = selectivities(n-1, k);
                selectivities(idx_to_remove, k) = selectivities(n-1, k);
            }
            selectivity_to_joined[idx_to_remove] = selectivity_to_joined[n-1];
            self_selectivities[idx_to_remove] = self_selectivities[n-1];
            sizes[idx_to_remove] = sizes[n-1];
            indexes[idx_to_remove] = indexes[n-1];
        }
        n -= 1;
    }
    return n;
}

static Float64 calculateCost(Float64 size_left, Float64 size_right, Float64 num_cols_right, Float64 sel_left, Float64 sel_right)
{
    /// The constants here come from a linear regression fitted to predict runtimes of joins of two tables.
    /// Use utils/tune-join-coefficients/run.sh to re-fit this model.
    return size_left * 0.01709865 +
           num_cols_right * size_right * 0.01674002 +
           size_left * sel_left * 0.06704978 +
           size_right * sel_right * 0.3567766;
}

static void updateCost(Float64 & total_cost, Float64 & size_left, Float64 size_right, Float64 num_cols_right, Float64 sel_left, Float64 sel_right)
{
    /// take into account that lhs costs less than rhs
    total_cost += calculateCost(size_left, size_right, num_cols_right, sel_left, sel_right);
    size_left *= size_right * sel_left * sel_right;
}

static Float64 greedy(const size_t N, /// Number of remaining tables to join
                      Float64 joined_size, /// Estimated cardinality of the set produced by joining tables in `join_order`
                      Float64 total_cost, /// Estimated cost of the doing the joins in `join_order`
                      std::vector<size_t> & join_order, /// Initial join order. Can be empty.
                      Vector2D<Float64> & selectivities, /// sel[i,j] = sel[j,i] = selectivity of predicates between i and j.
                      std::vector<Float64> & selectivity_to_joined, /// selectivity between current join tree and table i
                      std::vector<Float64> & self_selectivities, /// selectivity for non-join predicates on each table
                      std::vector<Float64> & sizes, /// size of each table
                      std::vector<Float64> & col_counts, /// number of used columns for each table
                      std::vector<size_t> & indexes) /// map from current working index to index in select.tables()
{
    /// Produces a left-deep tree by a greedy algorithm. O(N^2) runtime.
    /// Implementation inspired by Greedy Operator Ordering ("A New Heuristic for Optimizing Large Queries", Leonidas Fegaras, 1998).
    for (size_t n = N; n >= 1; n--) /// n is the number of remaining nodes to consider. n-1 is the last valid index.
    {
        /// Find the i that has the smallest value for selectivity.
        Float64 min_sel = std::numeric_limits<Float64>::infinity();
        size_t argmin_i = 0;
        for (size_t i = 0; i < n; i++)
        {
            if (selectivity_to_joined[i] < min_sel)
            {
                argmin_i = i;
                min_sel = selectivity_to_joined[i];
            }
        }

        updateCost(total_cost, joined_size, sizes[argmin_i], col_counts[argmin_i], selectivity_to_joined[argmin_i], self_selectivities[argmin_i]);
        join_order.push_back(indexes[argmin_i]); ///argmin_i needs to mapped to its original index in select.tables()

        //node i is now a part of the join tree, update selectivities for the remaining nodes.
        for (size_t k = 0; k < n; k++)
        {
            selectivity_to_joined[k] *= selectivities(argmin_i, k);
        }

        //move last node (index: n-1) to the place of selected node (index: argmin_i).
        if (argmin_i != n-1)
        {
            for (size_t k = 0; k < n; k++)
            {
                selectivities(k, argmin_i) = selectivities(n-1, k);
                selectivities(argmin_i, k) = selectivities(n-1, k);
            }
            selectivity_to_joined[argmin_i] = selectivity_to_joined[n-1];
            self_selectivities[argmin_i] = self_selectivities[n-1];
            sizes[argmin_i] = sizes[n-1];
            indexes[argmin_i] = indexes[n-1];
        }
    }
    return total_cost;
}

static std::vector<size_t> bruteForceOneThenGreedy(const size_t N, const std::vector<Float64> & sizes, const std::vector<Float64> & col_counts, const Vector2D<Float64> & selectivities, const std::vector<Float64> & self_selectivities)
{
    /// Runs a greedy algorithm, but considering every choice of starting node. Runtime: O(N^3)
    std::vector<size_t> indexes = range(N);
    Vector2D<Float64> selectivities_copy(N, N);
    std::vector<Float64> selectivity_to_joined = ones(N);
    std::vector<Float64> self_selectivities_copy = self_selectivities;
    std::vector<Float64> selectivity_to_joined_copy;
    std::vector<Float64> sizes_copy;
    std::vector<Float64> col_counts_copy;
    std::vector<size_t> indexes_copy;

    Float64 min_cost = std::numeric_limits<Float64>::infinity();
    std::vector<size_t> best_order;
    best_order.reserve(N);
    for (size_t i = 0; i < N; i++)
    {
        std::vector<size_t> join_order = {i};
        selectivities_copy = selectivities;
        selectivity_to_joined_copy = selectivity_to_joined;
        self_selectivities_copy = self_selectivities;
        sizes_copy = sizes;
        col_counts_copy = col_counts;
        indexes_copy = indexes;
        size_t new_n = removeElems(N, join_order, selectivities_copy, selectivity_to_joined_copy, self_selectivities_copy, sizes_copy, indexes_copy);
        Float64 cost = greedy(new_n, sizes[i], 1.0, join_order, selectivities_copy, selectivity_to_joined_copy, self_selectivities_copy, sizes_copy, col_counts_copy, indexes_copy);
        if (cost < min_cost)
        {
            min_cost = cost;
            best_order = join_order;
        }
    }
    return best_order;
}

static std::vector<size_t> bruteForceTwoThenGreedy(const size_t N, const std::vector<Float64> & sizes, const std::vector<Float64> & col_counts, const Vector2D<Float64> & selectivities, const std::vector<Float64> & self_selectivities)
{
    /// Runs a greedy algorithm, but considering every choice of pairs of starting nodes. Runtime: O(N^4)
    std::vector<size_t> indexes = range(N);
    Vector2D<Float64> selectivities_copy(N, N);
    std::vector<Float64> selectivity_to_joined = ones(N);
    std::vector<Float64> self_selectivities_copy = self_selectivities;
    std::vector<Float64> selectivity_to_joined_copy;
    std::vector<Float64> sizes_copy;
    std::vector<Float64> col_counts_copy;
    std::vector<size_t> indexes_copy;

    Float64 min_cost = std::numeric_limits<Float64>::infinity();
    std::vector<size_t> best_order;
    best_order.reserve(N);
    for (size_t ii = 1; ii < N; ii++)
    {
        for (size_t jj = 0; jj < ii; jj++)
        {
            Float64 cost_i_first = calculateCost(sizes[ii], sizes[jj], col_counts[jj], self_selectivities_copy[ii], self_selectivities_copy[jj]);
            Float64 cost_j_first = calculateCost(sizes[jj], sizes[ii], col_counts[ii], self_selectivities_copy[jj], self_selectivities_copy[ii]);
            size_t i, j;
            if (cost_i_first < cost_j_first) { i = ii; j = jj; }
            else { i = jj; j = ii; }
            std::vector<size_t> join_order = {i,j};
            join_order.reserve(N);

            selectivities_copy = selectivities;
            selectivity_to_joined_copy = selectivity_to_joined;
            self_selectivities_copy = self_selectivities;
            sizes_copy = sizes;
            col_counts_copy = col_counts;
            indexes_copy = indexes;
            Float64 total_cost = 1.0;
            Float64 joined_size = sizes[i];
            updateCost(total_cost, joined_size, sizes[j], col_counts[j], selectivities(i, j)*self_selectivities_copy[i], self_selectivities_copy[j]);
            size_t new_n = removeElems(N, join_order, selectivities_copy, selectivity_to_joined_copy, self_selectivities_copy, sizes_copy, indexes_copy);
            Float64 cost = greedy(new_n, joined_size, total_cost, join_order, selectivities_copy, selectivity_to_joined_copy, self_selectivities_copy, sizes_copy, col_counts_copy, indexes_copy);
            if (cost < min_cost)
            {
                min_cost = cost;
                best_order = join_order;
            }
        }
    }
    return best_order;
}

std::vector<size_t> findGoodReordering(const std::vector<Float64> sizes, const std::vector<Float64> col_counts, const Vector2D<Float64> & selectivities, const std::vector<Float64> & self_selectivities)
{
    const size_t N = sizes.size();
    /// FIXME: we can probably fully brute force all permutations up to a certain N. e.g. 8! = 40320, not too large.
    if (N < 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't reorder less than two tables");
    //bruteForceTwoThenGreedy has O(N^4) runtime. In a benchmark, reordering 16 tables took 0.16ms,
    //N=23 took 0.6ms, N=33 took 1.7ms, N=43 took 2.2ms, N=53 took 9.5ms, N=73 took 20ms, N=93 took 44ms, N=101 took 62ms.
    else if (N <= 100)
        return bruteForceTwoThenGreedy(N, sizes, col_counts, selectivities, self_selectivities); /// O(N^4)
    else
        return bruteForceOneThenGreedy(N, sizes, col_counts, selectivities, self_selectivities); /// O(N^3)
}

}
