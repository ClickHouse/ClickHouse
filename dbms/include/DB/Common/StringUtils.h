#pragma once

#include <string>
#include <cstring>


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


/// strlen evaluated compile-time.
inline bool startsWith(const std::string & s, const char * prefix)
{
	return detail::startsWith(s, prefix, strlen(prefix));
}

inline bool endsWith(const std::string & s, const char * suffix)
{
	return detail::endsWith(s, suffix, strlen(suffix));
}
