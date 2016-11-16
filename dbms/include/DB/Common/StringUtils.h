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


/// More efficient than libc, because doesn't respect locale.

inline bool isASCII(char c)
{
	return static_cast<unsigned char>(c) < 0x80;
}

inline bool isAlphaASCII(char c)
{
	return (c >= 'a' && c <= 'z')
		|| (c >= 'A' && c <= 'Z');
}

inline bool isNumericASCII(char c)
{
	return (c >= '0' && c <= '9');
}

inline bool isAlphaNumericASCII(char c)
{
	return isAlphaASCII(c)
		|| isNumericASCII(c);
}

inline bool isWordCharASCII(char c)
{
	return isAlphaNumericASCII(c)
		|| c == '_';
}

inline bool isWhitespaceASCII(char c)
{
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v';
}

/// Works assuming isAlphaASCII.
inline char toLowerIfAlphaASCII(char c)
{
	return c | 0x20;
}

inline char toUpperIfAlphaASCII(char c)
{
	return c & (~0x20);
}

inline char alternateCaseIfAlphaASCII(char c)
{
	return c ^ 0x20;
}
