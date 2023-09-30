#pragma once

/// Extend @p to by moving elements from @p from to @p to end
/// @return @p to iterator to first of moved elements.
template <class To, class From>
typename To::iterator moveExtend(To & to, From && from)
{
    return to.insert(to.end(), std::make_move_iterator(from.begin()), std::make_move_iterator(from.end()));
}
