#pragma once

#include <DB/Common/Exception.h>
#include <string>
#include <cstring>

namespace DB { namespace ErrorCodes {

extern const int LOGICAL_ERROR;

}}

namespace detail
{
	bool startsWith(const std::string & s, const char * prefix, size_t prefix_size);
	bool endsWith(const std::string & s, const char * suffix, size_t suffix_size);
}


inline bool startsWith(const std::string & s, const std::string & prefix)
{
	return detail::startsWith(s, prefix.data(), prefix.size());
}

inline bool endsWith(const std::string & s, const std::string & suffix)
{
	return detail::endsWith(s, suffix.data(), suffix.size());
}


/// With GCC, strlen is evaluated compile time if we pass it a constant
/// string that is known at compile time.
inline bool startsWith(const std::string & s, const char * prefix)
{
	return detail::startsWith(s, prefix, strlen(prefix));
}

inline bool endsWith(const std::string & s, const char * suffix)
{
	return detail::endsWith(s, suffix, strlen(suffix));
}

/// Given an integer, return the adequate suffix for
/// printing an ordinal number.
template <typename T>
std::string getOrdinalSuffix(T n)
{
	static_assert(std::is_integral<T>::value, "Integer value required");

	auto val = n % 10;

	bool is_th;
	if ((val >= 1) && (val <= 3))
		is_th = (n > 10) && (((n / 10) % 10) == 1);
	else
		is_th = true;

	if (is_th)
		return "th";
	else
	{
		switch (val)
		{
			case 1: return "st";
			case 2: return "nd";
			case 3: return "rd";
			default: throw DB::Exception{"Internal error", DB::ErrorCodes::LOGICAL_ERROR};
		};
	}
}
