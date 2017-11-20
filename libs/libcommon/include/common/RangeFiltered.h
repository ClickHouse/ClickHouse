#pragma once
#include <type_traits>


/// Similar to boost::filtered_range but a little bit easier and allows to convert ordinary iterators to filtered
template <typename F, typename C>
struct RangeFiltered
{
    /// Template parameter C may be const. Then const_iterator is used.
    using RawIterator = decltype(std::declval<C>().begin());
    class Iterator;

    /// Will iterate over elements for which filter(*it) == true
    template <typename F_, typename C_> /// Another template for universal references to work.
    RangeFiltered(F_ && filter, C_ && container)
        : filter(std::move(filter)), container(container) {}

    Iterator begin() const
    {
        return Iterator{*this, std::begin(container)};
    }

    Iterator end() const
    {
        return Iterator{*this, std::end(container)};
    }

    /// Convert ordinary iterator to filtered one
    /// Real position will be in range [ordinary_iterator; end()], so it is suitable to use with lower[upper]_bound()
    inline Iterator convert(RawIterator ordinary_iterator) const
    {
        return Iterator{*this, ordinary_iterator};
    }


    /// It is similar to boost::filtered_iterator, but has additional features:
    ///  it doesn't store end() iterator
    ///  it doesn't store predicate, so it allows to implement operator=()
    ///  it guarantees that operator++() works properly in case of filter(*it) == false
    class Iterator
    {
    public:
        using Range = RangeFiltered<F, C>;

        typedef Iterator self_type;
        typedef typename std::iterator_traits<RawIterator>::value_type value_type;
        typedef typename std::iterator_traits<RawIterator>::reference reference;
        typedef const value_type & const_reference;
        typedef typename std::iterator_traits<RawIterator>::pointer pointer;
        typedef const value_type * const_pointer;
        typedef typename std::iterator_traits<RawIterator>::difference_type difference_type;
        typedef std::bidirectional_iterator_tag iterator_category;

        Iterator(const Range & range_, RawIterator iter_)
        : range(&range_), iter(iter_)
        {
            for (; iter != std::end(range->container) && !range->filter(*iter); ++iter);
        }

        Iterator(const Iterator & rhs) = default;
        Iterator(Iterator && rhs) noexcept = default;

        Iterator operator++()
        {
            ++iter;
            for (; iter != std::end(range->container) && !range->filter(*iter); ++iter);
            return *this;
        }

        Iterator operator--()
        {
            --iter;
            for (; !range->filter(*iter); --iter); /// Don't check std::begin() bound
            return *this;
        }

        pointer operator->()
        {
            return iter.operator->();
        }

        const_pointer operator->() const
        {
            return iter.operator->();
        }

        reference operator*()
        {
            return *iter;
        }

        const_reference operator*() const
        {
            return *iter;
        }

        bool operator==(const self_type & rhs) const
        {
            return iter == rhs.iter;
        }

        bool operator!=(const self_type & rhs) const
        {
            return iter != rhs.iter;
        }

        const RawIterator & base() const
        {
            return iter;
        }

        self_type & operator=(const self_type & rhs) = default;
        self_type & operator=(self_type && rhs) noexcept = default;

        ~Iterator() = default;

    private:
        const Range * range = nullptr;
        RawIterator iter;
    };

protected:
    F filter;
    C & container;
};


template <typename F, typename C>
inline RangeFiltered<std::decay_t<F>, std::remove_reference_t<C>> createRangeFiltered(F && filter, C && container)
{
    return {std::forward<F>(filter), std::forward<C>(container)};
};
