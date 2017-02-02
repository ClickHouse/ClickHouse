#pragma once

#include <cstring>
#include <string>

struct StringRef
{
	const char * data = nullptr;
	size_t size = 0;

	StringRef() {}
	StringRef(const char * c_str) { *this = c_str; }
	StringRef & operator= (const char * c_str) { data = c_str; size = strlen(c_str); return *this; }

	std::string toString() const { return {data, size}; }

	bool operator== (const char * rhs) const
	{
		return size == strlen(rhs) && 0 == memcmp(data, rhs, strlen(rhs));
	}

	operator bool() const { return data != nullptr; }
};
