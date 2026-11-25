#pragma once

#include <algorithm>
#include <cstddef>
#include <iterator>
#include <string_view>
#include <utility>

/// Depends only on the standard library: it is shared by `SymbolIndex` and the standalone `compact-symbols` tool.

namespace DB
{

/// Whether `lhs` is preferred to `rhs` as the name of an address range that both name, as with aliases and
/// functions merged by identical code folding. The preferred name is not empty, then has fewer leading '_' bytes,
/// then is shorter, then is smaller in unsigned byte order. A strict total order on names: two distinct names are
/// never equivalent. Readers do not drop empty names, so that they do not read the names of every symbol: an
/// empty name loses its range to any other one here.
constexpr bool isPreferredSymbolName(std::string_view lhs, std::string_view rhs)
{
    if (lhs.empty() != rhs.empty())
        return rhs.empty();

    size_t lhs_underscores = std::min(lhs.find_first_not_of('_'), lhs.size());
    size_t rhs_underscores = std::min(rhs.find_first_not_of('_'), rhs.size());
    if (lhs_underscores != rhs_underscores)
        return lhs_underscores < rhs_underscores;
    if (lhs.size() != rhs.size())
        return lhs.size() < rhs.size();
    return lhs.compare(rhs) < 0;
}

/// Like `std::unique`, but keeps from each run of consecutive elements for which `same_range` holds the one that
/// `prefer` puts first, rather than the first one. `prefer(lhs, rhs)` must be a strict total order on the elements
/// of one run for the result not to depend on their order; it is called only for elements of runs longer than one.
template <typename ForwardIt, typename SameRange, typename Prefer>
ForwardIt keepPreferredPerRange(ForwardIt first, ForwardIt last, SameRange && same_range, Prefer && prefer)
{
    ForwardIt result = first;
    while (first != last)
    {
        ForwardIt best = first;
        ForwardIt next = std::next(first);
        for (; next != last && same_range(*first, *next); ++next)
            if (prefer(*next, *best))
                best = next;

        if (result != best)
            *result = std::move(*best);
        ++result;
        first = next;
    }
    return result;
}

}
