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
		StringRef(const unsigned char * data_, size_t size_) : data(reinterpret_cast<const char *>(data_)), size(size_) {}
		StringRef(const std::string & s) : data(s.data()), size(s.size()) {}
		StringRef() : data(NULL), size(0) {}
	};

	inline bool operator==(StringRef lhs, StringRef rhs)
	{
		/// Так почему-то быстрее, чем return lhs.size == rhs.size && 0 == memcmp(lhs.data, rhs.data, lhs.size);
		
		if (lhs.size != rhs.size)
			return false;

		for (size_t pos = 0; pos < lhs.size; ++pos)
			if (lhs.data[pos] != rhs.data[pos])
				return false;

		return true;
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
