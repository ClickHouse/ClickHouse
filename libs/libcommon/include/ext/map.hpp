#pragma once

#include <iterator>
#include <type_traits>

namespace ext
{
	/// \brief Strip type off top level reference and cv-qualifiers thus allowing storage in containers
	template <typename T>
	using unqualified_t = std::remove_cv_t<std::remove_reference_t<T>>;

	template <typename It, typename Mapper>
	using apply_t = typename std::result_of<Mapper(typename It::reference)>::type;

	template <typename It, typename Mapper>
	struct map_iterator : std::iterator<
		typename It::iterator_category,
		std::remove_reference_t<apply_t<It, Mapper>>,
		std::ptrdiff_t,
		std::add_pointer_t<std::remove_reference_t<apply_t<It, Mapper>>>,
		apply_t<It, Mapper>>
	{
		using base_iterator = std::iterator<
			typename It::iterator_category,
			std::remove_reference_t<apply_t<It, Mapper>>,
			std::ptrdiff_t,
			std::add_pointer<std::remove_reference_t<apply_t<It, Mapper>>>,
			apply_t<It, Mapper>>;

		It current;
		Mapper mapper;

		map_iterator(const It it, const Mapper mapper) : current{it}, mapper{mapper} {}

		typename base_iterator::reference operator*() { return mapper(*current); }

		map_iterator & operator++() { return ++current, *this; }
		map_iterator & operator--() { return --current, *this; }

		bool operator==(const map_iterator & other) { return current == other.current; }
		bool operator!=(const map_iterator & other) { return current != other.current; }

		typename base_iterator::difference_type operator-(const map_iterator & other) { return current - other.current; }
	};

	template <typename It, typename Mapper>
	auto make_map_iterator(const It it, const Mapper mapper) -> ext::map_iterator<It, Mapper>
	{
		return { it, mapper };
	}

	/** \brief Returns collection of the same container-type as the input collection,
	*	with each element transformed by the application of `mapper`. */
	template <template <typename...> class Collection, typename... Params, typename Mapper>
	auto map(const Collection<Params...> & collection, const Mapper mapper)
	{
		using value_type = unqualified_t<decltype(mapper(*std::begin(collection)))>;

		return Collection<value_type>(ext::make_map_iterator(std::begin(collection), mapper),
			ext::make_map_iterator(std::end(collection), mapper));
	};

	/** \brief Returns collection of specified container-type,
	*	with each element transformed by the application of `mapper`.
	*	Allows conversion between different container-types, e.g. std::vector to std::list */
	template <template <typename...> class ResultCollection, typename Collection, typename Mapper>
	auto map(const Collection & collection, const Mapper mapper)
	{
		using value_type = unqualified_t<decltype(mapper(*std::begin(collection)))>;

		return ResultCollection<value_type>(ext::make_map_iterator(std::begin(collection), mapper),
			ext::make_map_iterator(std::end(collection), mapper));
	};

	/** \brief Returns collection of specified type,
	*	with each element transformed by the application of `mapper`.
	*	Allows leveraging implicit conversion between the result of applying `mapper` and R::value_type. */
	template <typename ResultCollection, typename Collection, typename Mapper>
	auto map(const Collection & collection, const Mapper mapper)
	{
		return ResultCollection(ext::make_map_iterator(std::begin(collection), mapper),
			ext::make_map_iterator(std::end(collection), mapper));
	}
}
