#pragma once

#include <type_traits>


namespace ext
{
	template <typename To, typename From>
	union bit_cast_helper_t
	{
		using dst = std::decay_t<To>;
		using src = std::decay_t<From>;

		static_assert(std::is_copy_constructible<dst>::value, "destination type must be CopyConstructible");
		static_assert(std::is_copy_constructible<src>::value, "source type must be CopyConstructible");

		/** value-initialize member `to` first in case destination type is larger than source */
		dst to{};
		src from;

		bit_cast_helper_t(const From & from) : from{from} {}

		operator dst() const { return to; }
	};

	/** \brief Returns value `from` converted to type `To` while retaining bit representation.
	 *	`To` and `From` must satisfy `CopyConstructible`. */
	template <typename To, typename From>
	std::decay_t<To> bit_cast(const From & from)
	{
		return bit_cast_helper_t<To, From>{from};
	};

	/** \brief Returns value `from` converted to type `To` while retaining bit representation.
	 *	`To` and `From` must satisfy `CopyConstructible`. */
	template <typename To, typename From>
	std::decay_t<To> safe_bit_cast(const From & from)
	{
		static_assert(sizeof(To) == sizeof(From), "bit cast on types of different width");
		return bit_cast_helper_t<To, From>{from};
	};
}
