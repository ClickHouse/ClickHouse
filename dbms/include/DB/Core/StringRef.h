#pragma once

#include <string.h>
#include <city.h>

#include <string>


namespace DB
{
	/// Штука, чтобы не создавать строки для поиска подстроки в хэш таблице.
	struct StringRef
	{
		const char * data;
		size_t size;

		StringRef(const char * data_, size_t size_) : data(data_), size(size_) {}
		StringRef(const std::string & s) : data(s.data()), size(s.size()) {}
		StringRef() : data(NULL), size(0) {}
	};

	inline bool operator==(StringRef lhs, StringRef rhs)
	{
		return lhs.size == rhs.size && 0 == memcmp(lhs.data, rhs.data, lhs.size);
	}

	inline bool operator!=(StringRef lhs, StringRef rhs)
	{
		return !(lhs == rhs);
	}

	struct StringRefHash
	{
		inline size_t operator() (StringRef x) const
		{
			return CityHash64(x.data, x.size);
		}
	};
}
