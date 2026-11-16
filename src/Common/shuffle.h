#pragma once

#include <iterator>
#include <random>
#include <utility>

/* Reorders the elements in the given range [first, last) such that each
 * possible permutation of those elements has equal probability of appearance.
 *
 * for i ∈ [0, n-2):
 *     j ← random from ∈ [i, n)
 *     swap arr[i] ↔ arr[j]
 */
template <typename Iter, typename Rng>
void shuffle(Iter first, Iter last, Rng && rng)
{
    using diff_t = typename std::iterator_traits<Iter>::difference_type;
    using distr_t = std::uniform_int_distribution<diff_t>;
    using param_t = typename distr_t::param_type;
    distr_t d;
    diff_t n = last - first;
    for (ssize_t i = 0; i < n - 1; ++i)
    {
        using std::swap;
        auto j = d(rng, param_t(i, n - 1));
        swap(first[i], first[j]);
    }
}


/* Partially shuffle elements in range [first, last) in such a way that
 * [first, first + limit) is a random subset of the original range.
 * [first + limit, last) shall contain the elements not in [first, first + limit)
 * in undefined order.
 *
 * for i ∈ [0, limit):
 *     j ← random from ∈ [i, n)
 *     swap arr[i] ↔ arr[j]
 */
template <typename Iter, typename Rng>
void partial_shuffle(Iter first, Iter last, size_t limit, Rng && rng)
{
    using diff_t = typename std::iterator_traits<Iter>::difference_type;
    using distr_t = std::uniform_int_distribution<diff_t>;
    using param_t = typename distr_t::param_type;
    distr_t d;
    diff_t n = last - first;
    for (size_t i = 0; i < limit; ++i)
    {
        using std::swap;
        auto j = d(rng, param_t(i, n - 1));
        swap(first[i], first[j]);
    }
}
