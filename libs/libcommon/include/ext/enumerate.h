#pragma once

#include <ext/size.h>
#include <type_traits>
#include <utility>
#include <iterator>


/** \brief Provides a wrapper view around a container, allowing to iterate over it's elements and indices.
  *    Allow writing code like shown below:
  *
  *        std::vector<T> v = getVector();
  *        for (const std::pair<const std::size_t, T &> index_and_value : ext::enumerate(v))
  *            std::cout << "element " << index_and_value.first << " is " << index_and_value.second << std::endl;
  */
namespace ext
{
    template <typename It> struct enumerate_iterator
    {
        using traits = typename std::iterator_traits<It>;
        using iterator_category = typename traits::iterator_category;
        using value_type = std::pair<const std::size_t, typename traits::value_type>;
        using difference_type = typename traits::difference_type;
        using reference = std::pair<const std::size_t, typename traits::reference>;

        std::size_t idx;
        It it;

        enumerate_iterator(const std::size_t idx, It it) : idx{idx}, it{it} {}

        auto operator*() const { return reference(idx, *it); }

        bool operator!=(const enumerate_iterator & other) const { return it != other.it; }

        enumerate_iterator & operator++() { return ++idx, ++it, *this; }
    };

    template <typename Collection> struct enumerate_wrapper
    {
        using underlying_iterator = decltype(std::begin(std::declval<Collection &>()));
        using iterator = enumerate_iterator<underlying_iterator>;

        Collection & collection;

        enumerate_wrapper(Collection & collection) : collection(collection) {}

        auto begin() { return iterator(0, std::begin(collection)); }
        auto end() { return iterator(ext::size(collection), std::end(collection)); }
    };

    template <typename Collection> auto enumerate(Collection & collection)
    {
        return enumerate_wrapper<Collection>{collection};
    }

    template <typename Collection> auto enumerate(const Collection & collection)
    {
        return enumerate_wrapper<const Collection>{collection};
    }
}
