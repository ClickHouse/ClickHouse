#pragma once

#include <type_traits>
#include <utility>
#include <iterator>

/** \brief Numeric range iterator, used to represent a half-closed interval [begin, end).
 *	In conjunction with std::reverse_iterator allows for forward and backward iteration
 *	over corresponding interval. */
namespace ext
{
	/// @todo check whether difference_type should be signed (make_signed_t<T>)
	template<typename T> struct range_iterator : std::iterator<
		std::bidirectional_iterator_tag, T, T, void, T>
	{
		T current{};

		range_iterator() = default;
		range_iterator(const T t) : current(t) {}

		T operator*() const { return current; }

		range_iterator & operator++() { return ++current, *this; }
		range_iterator & operator--() { return --current, *this; }

		bool operator==(const range_iterator & other) const { return current == other.current; }
		bool operator!=(const range_iterator & other) const { return current != other.current; }
	};

	template<typename T> using reverse_range_iterator = std::reverse_iterator<range_iterator<T>>;

	/** \brief Range-based for loop adapter for (reverse_)range_iterator.
	 *  By and large should be in conjunction with ext::range and ext::reverse_range. */
	template<typename T, bool forward> struct range_wrapper
	{
		using value_type = typename std::remove_reference<T>::type;
		using range_iterator_t = ext::range_iterator<value_type>;
		using iterator = typename std::conditional<forward,
			range_iterator_t,
			ext::reverse_range_iterator<value_type>>::type;

		value_type begin_, end_;

		iterator begin() const { return iterator(range_iterator_t{begin_}); }
		iterator end() const { return iterator(range_iterator_t{end_}); }
	};

	/** \brief Constructs range_wrapper for forward-iteration over [begin, end) in range-based for loop.
	 *  Usage example:
	 *      for (const auto i : ext::range(0, 4)) print(i);
	 *  Output:
	 *      0 1 2 3 */
	template<typename T1, typename T2>
	inline ext::range_wrapper<typename std::common_type<T1, T2>::type, true> range(T1 begin, T2 end)
	{
		using common_type = typename std::common_type<T1, T2>::type;
		return { static_cast<common_type>(begin), static_cast<common_type>(end) };
	}

	/** \brief Constructs range_wrapper for backward-iteration over [begin, end) in range-based for loop.
	 *  Usage example:
	 *      for (const auto i : ext::reverse_range(0, 4)) print(i);
	 *  Output:
	 *      3 2 1 0 */
	template<typename T1, typename T2>
	inline ext::range_wrapper<typename std::common_type<T1, T2>::type, false> reverse_range(T1 begin, T2 end)
	{
		using common_type = typename std::common_type<T1, T2>::type;
		return { static_cast<common_type>(end), static_cast<common_type>(begin) };
	}

	template<typename T>
	inline ext::range_iterator<T> make_range_iterator(const T value)
	{
		return { value };
	}
}
