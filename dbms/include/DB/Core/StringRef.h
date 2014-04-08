#pragma once

#include <string.h>
#include <city.h>

#include <string>
#include <vector>

#include <functional>


namespace DB
{
	/// Штука, чтобы не создавать строки для поиска подстроки в хэш таблице.
	struct StringRef
	{
		const char * data = nullptr;
		size_t size = 0;

		StringRef(const char * data_, size_t size_) : data(data_), size(size_) {}
		StringRef(const unsigned char * data_, size_t size_) : data(reinterpret_cast<const char *>(data_)), size(size_) {}
		StringRef(const std::string & s) : data(s.data()), size(s.size()) {}
		StringRef() {}

		std::string toString() const { return std::string(data, size); }
	};

	typedef std::vector<StringRef> StringRefs;

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

	struct StringRefZeroTraits
	{
		static inline bool check(DB::StringRef x) { return nullptr == x.data; }
		static inline void set(DB::StringRef & x) { x.data = nullptr; }
	};
	
	inline bool operator==(StringRef lhs, const char * rhs)
	{
		for (size_t pos = 0; pos < lhs.size; ++pos)
		{
			if (!rhs[pos] || lhs.data[pos] != rhs[pos])
				return false;
		}
		return true;
	}

	inline bool operator<(StringRef lhs, StringRef rhs)
	{
		return strcmp(lhs.data, rhs.data) < 0 ? true : false;
	}

	inline std::ostream & operator<<(std::ostream & os, const StringRef & str)
	{
		if (str.data)
			return os << str.toString();
		else
			return os;
	}
}

namespace std
{
	template <>
	struct hash<DB::StringRef>
	{
		size_t operator()(const DB::StringRef & x) const
		{
			return CityHash64(x.data, x.size);
		}
	};
}
