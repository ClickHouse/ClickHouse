#pragma once
#include <type_traits>
#include <boost/range/adaptor/filtered.hpp>


/// Similar to boost::filtered_range but a little bit easier and allows to convert ordinary iterators to filtered
template <typename F, typename C>
struct RangeFiltered : public boost::iterator_range<boost::filter_iterator<F, decltype(std::end(C()))>>
{
    using RawIterator = decltype(std::end(C()));
    using FilterIterator = boost::filter_iterator<F, RawIterator>;
    using Base = boost::iterator_range<FilterIterator>;

    RangeFiltered(const F & filter, const C & container)
            : Base(
                FilterIterator(filter, std::begin(container), std::end(container)),
                FilterIterator(filter, std::end(container), std::end(container))),
              filter(filter) {}

    /// Convert ordinary iterator to filtered one
    /// Real position will be in range [ordinary_iterator; end()], so it is suitable to use with lower[upper]_bound()
    inline FilterIterator convert(RawIterator ordinary_iterator) const
    {
        return {filter, ordinary_iterator, std::end(*this).end()};
    }

    F filter;
};

template <typename F, typename C>
inline decltype(auto) createRangeFiltered(F && filter, C && container)
{
    return RangeFiltered<std::decay_t<F>, std::decay_t<C>>{std::forward<F>(filter), std::forward<C>(container)};
};
